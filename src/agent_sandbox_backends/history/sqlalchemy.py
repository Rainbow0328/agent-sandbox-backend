from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    delete,
    func,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.errors import HistoryDatabaseError
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.operations import OperationEvent, OperationStatus
from agent_sandbox_backends.history.config import HistoryConfig

metadata = MetaData()

history_events = Table(
    "history_events",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("provider_key", String(255), nullable=False),
    Column("sandbox_id", String(255), nullable=False),
    Column("sandbox_instance_id", String(255), nullable=False),
    Column("actor_type", String(32), nullable=False),
    Column("actor_id", String(255)),
    Column("thread_id", String(255)),
    Column("run_id", String(255)),
    Column("correlation_id", String(255), nullable=False),
    Column("operation_type", String(128), nullable=False),
    Column("status", String(32), nullable=False),
    Column("occurred_at", String(64), nullable=False),
    Column("completed_at", String(64)),
    Column("request_json", JSON),
    Column("result_json", JSON),
    Column("duration_ms", BigInteger),
    Column("error_code", String(128)),
    Column("schema_version", Integer, nullable=False),
)

history_changes = Table(
    "history_changes",
    metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("change_id", String(255), nullable=False, unique=True),
    Column("event_id", String(64), nullable=False),
    Column("change_type", String(32), nullable=False),
    Column("changed_at", String(64), nullable=False),
    Column("schema_version", Integer, nullable=False),
)

command_output_chunks = Table(
    "command_output_chunks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", String(64), nullable=False),
    Column("stream", String(16), nullable=False),
    Column("chunk_index", Integer, nullable=False),
    Column("data", LargeBinary, nullable=False),
    Column("created_at", String(64), nullable=False),
    UniqueConstraint("event_id", "stream", "chunk_index"),
)

history_consumers = Table(
    "history_consumers",
    metadata,
    Column("consumer_id", String(255), primary_key=True),
    Column("acknowledged_seq", BigInteger, nullable=False),
    Column("updated_at", String(64), nullable=True),
)

history_leases = Table(
    "history_leases",
    metadata,
    Column("resource_key", String(512), primary_key=True),
    Column("owner_id", String(255), nullable=False),
    Column("expires_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)


class SQLAlchemyHistoryStore:
    """Canonical async database history for PostgreSQL and async SQLite."""

    def __init__(
        self,
        url: str,
        *,
        identity: SandboxRef,
        config: HistoryConfig | None = None,
        engine: AsyncEngine | None = None,
    ) -> None:
        if not url.startswith(("postgresql+asyncpg://", "sqlite+aiosqlite://")):
            raise ValueError(
                "Database history URL must use postgresql+asyncpg or sqlite+aiosqlite"
            )
        self.url = url
        self.identity = identity
        self.config = config or HistoryConfig()
        self.engine = engine or create_async_engine(url, pool_pre_ping=True)
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        try:
            async with self.engine.begin() as connection:
                await connection.run_sync(metadata.create_all)
        except SQLAlchemyError as error:
            raise self._database_error("history.initialize", error) from error
        self._initialized = True

    async def append(self, event: OperationEvent) -> None:
        await self.initialize()
        try:
            async with self.engine.begin() as connection:
                existing = (
                    await connection.execute(
                        select(
                            history_events.c.operation_type,
                            history_events.c.status,
                        ).where(history_events.c.event_id == event.event_id)
                    )
                ).mappings().first()
                if event.status == OperationStatus.STARTED:
                    if existing is None:
                        await connection.execute(
                            insert(history_events).values(**self._started(event))
                        )
                    elif existing["operation_type"] != event.operation_type:
                        raise HistoryDatabaseError("Conflicting duplicate operation start")
                    await self._append_change(
                        connection,
                        change_id=f"{event.event_id}:created",
                        event=event,
                        change_type="created",
                    )
                    return
                if existing is None:
                    raise HistoryDatabaseError("Terminal history event has no started event")
                output_added = await self._append_output(connection, event)
                if output_added:
                    await self._append_change(
                        connection,
                        change_id=f"{event.event_id}:output",
                        event=event,
                        change_type="output_appended",
                    )
                await connection.execute(
                    update(history_events)
                    .where(history_events.c.event_id == event.event_id)
                    .values(
                        status=event.status.value,
                        completed_at=self._time(event),
                        result_json=event.result,
                        duration_ms=event.duration_ms,
                        error_code=event.error_code,
                    )
                )
                await self._append_change(
                    connection,
                    change_id=f"{event.event_id}:completed:{event.status.value}",
                    event=event,
                    change_type="completed",
                )
        except HistoryDatabaseError:
            raise
        except SQLAlchemyError as error:
            raise self._database_error(event.operation_type, error) from error

    async def append_output(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> None:
        if not chunks:
            return
        await self.initialize()
        changed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        inserted_indexes: list[str] = []
        try:
            async with self.engine.begin() as connection:
                existing_event = (
                    await connection.execute(
                        select(history_events.c.event_id).where(
                            history_events.c.event_id == event_id
                        )
                    )
                ).first()
                if existing_event is None:
                    raise HistoryDatabaseError("Output history has no started event")
                for chunk in chunks:
                    existing_chunk = (
                        await connection.execute(
                            select(command_output_chunks.c.id).where(
                                command_output_chunks.c.event_id == event_id,
                                command_output_chunks.c.stream == chunk.stream.value,
                                command_output_chunks.c.chunk_index == chunk.chunk_index,
                            )
                        )
                    ).first()
                    if existing_chunk is not None:
                        continue
                    await connection.execute(
                        insert(command_output_chunks).values(
                            event_id=event_id,
                            stream=chunk.stream.value,
                            chunk_index=chunk.chunk_index,
                            data=chunk.data,
                            encoding="identity",
                            original_size=len(chunk.data),
                            created_at=changed_at,
                        )
                    )
                    inserted_indexes.append(f"{chunk.stream.value}:{chunk.chunk_index}")
                if inserted_indexes:
                    await connection.execute(
                        insert(history_changes).values(
                            change_id=f"{event_id}:output:{','.join(inserted_indexes)}",
                            event_id=event_id,
                            change_type="output_appended",
                            changed_at=changed_at,
                            schema_version=1,
                        )
                    )
        except HistoryDatabaseError:
            raise
        except SQLAlchemyError as error:
            raise self._database_error("history.output", error) from error

    async def query_changes(
        self,
        *,
        after_change_seq: int = 0,
        limit: int = 100,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        del max_bytes
        await self.initialize()
        try:
            async with self.engine.connect() as connection:
                rows = (
                    await connection.execute(
                        select(
                            history_changes.c.seq.label("source_seq"),
                            history_changes.c.change_id,
                            history_changes.c.event_id,
                            history_changes.c.change_type,
                            history_changes.c.changed_at,
                            history_changes.c.schema_version,
                            history_events.c.operation_type,
                            history_events.c.status,
                        )
                        .join(
                            history_events,
                            history_events.c.event_id == history_changes.c.event_id,
                        )
                        .where(history_changes.c.seq > after_change_seq)
                        .order_by(history_changes.c.seq)
                        .limit(limit)
                    )
                ).mappings().all()
        except SQLAlchemyError as error:
            raise self._database_error("history.query", error) from error
        changes = [dict(row) for row in rows]
        maximum = max((int(row["source_seq"]) for row in changes), default=after_change_seq)
        return {"changes": changes, "max_change_seq": maximum, "reset_required": False}

    async def get_output(
        self,
        event_id: str,
        stream: str,
        *,
        after_chunk: int = -1,
    ) -> dict[str, Any]:
        await self.initialize()
        try:
            async with self.engine.connect() as connection:
                rows = (
                    await connection.execute(
                        select(
                            command_output_chunks.c.chunk_index,
                            command_output_chunks.c.data,
                        )
                        .where(
                            command_output_chunks.c.event_id == event_id,
                            command_output_chunks.c.stream == stream,
                            command_output_chunks.c.chunk_index > after_chunk,
                        )
                        .order_by(command_output_chunks.c.chunk_index)
                    )
                ).mappings().all()
        except SQLAlchemyError as error:
            raise self._database_error("history.get_output", error) from error
        return {
            "event_id": event_id,
            "stream": stream,
            "chunks": [
                {
                    "chunk_index": int(row["chunk_index"]),
                    "data_base64": base64.b64encode(row["data"]).decode("ascii"),
                }
                for row in rows
            ],
        }

    async def get_operation(self, event_id: str) -> dict[str, Any]:
        await self.initialize()
        try:
            async with self.engine.connect() as connection:
                row = (
                    await connection.execute(
                        select(history_events).where(history_events.c.event_id == event_id)
                    )
                ).mappings().first()
        except SQLAlchemyError as error:
            raise self._database_error("history.get_operation", error) from error
        if row is None:
            raise HistoryDatabaseError(f"Operation does not exist: {event_id}")
        result = dict(row)
        result["request"] = result.pop("request_json")
        result["result"] = result.pop("result_json")
        return result

    async def acknowledge(self, consumer_id: str, seq: int) -> dict[str, Any]:
        await self.initialize()
        now_text = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        try:
            async with self.engine.begin() as connection:
                insert_factory = (
                    postgresql_insert
                    if self.engine.dialect.name == "postgresql"
                    else sqlite_insert
                )
                statement = insert_factory(history_consumers).values(
                    consumer_id=consumer_id,
                    acknowledged_seq=seq,
                    updated_at=now_text,
                )
                maximum = (
                    func.greatest(
                        history_consumers.c.acknowledged_seq,
                        statement.excluded.acknowledged_seq,
                    )
                    if self.engine.dialect.name == "postgresql"
                    else func.max(
                        history_consumers.c.acknowledged_seq,
                        statement.excluded.acknowledged_seq,
                    )
                )
                await connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=[history_consumers.c.consumer_id],
                        set_={
                            "acknowledged_seq": maximum,
                            "updated_at": statement.excluded.updated_at,
                        },
                    )
                )
                acknowledged = int(
                    (
                        await connection.execute(
                            select(history_consumers.c.acknowledged_seq).where(
                                history_consumers.c.consumer_id == consumer_id
                            )
                        )
                    ).scalar_one()
                )
        except SQLAlchemyError as error:
            raise self._database_error("history.ack", error) from error
        return {"consumer_id": consumer_id, "acknowledged_seq": acknowledged}

    async def lease_acquire(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        await self.initialize()
        now = datetime.now(UTC)
        expires_at = (now + timedelta(milliseconds=ttl_ms)).isoformat().replace("+00:00", "Z")
        now_text = now.isoformat().replace("+00:00", "Z")
        current_owner: str | None = None
        current_expires_at: str | None = None
        try:
            async with self.engine.begin() as connection:
                values = {
                    "resource_key": resource_key,
                    "owner_id": owner_id,
                    "expires_at": expires_at,
                    "updated_at": now_text,
                }
                insert_factory = (
                    postgresql_insert
                    if self.engine.dialect.name == "postgresql"
                    else sqlite_insert
                )
                statement = insert_factory(history_leases).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=[history_leases.c.resource_key],
                    set_={
                        "owner_id": owner_id,
                        "expires_at": expires_at,
                        "updated_at": now_text,
                    },
                    where=or_(
                        history_leases.c.owner_id == owner_id,
                        history_leases.c.expires_at <= now_text,
                    ),
                )
                result = await connection.execute(statement)
                acquired = result.rowcount == 1
                if not acquired:
                    current = (
                        await connection.execute(
                            select(history_leases).where(
                                history_leases.c.resource_key == resource_key
                            )
                        )
                    ).mappings().one()
                    current_owner = str(current["owner_id"])
                    current_expires_at = str(current["expires_at"])
        except SQLAlchemyError as error:
            raise self._database_error("history.lease_acquire", error) from error
        if not acquired:
            if current_owner is None or current_expires_at is None:
                raise HistoryDatabaseError("Lease conflict row could not be read")
            return {
                "acquired": False,
                "resource_key": resource_key,
                "owner_id": current_owner,
                "expires_at": current_expires_at,
            }
        return {
            "acquired": True,
            "resource_key": resource_key,
            "owner_id": owner_id,
            "expires_at": expires_at,
        }

    async def lease_renew(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        await self.initialize()
        now = datetime.now(UTC)
        expires_at = (now + timedelta(milliseconds=ttl_ms)).isoformat().replace("+00:00", "Z")
        now_text = now.isoformat().replace("+00:00", "Z")
        try:
            async with self.engine.begin() as connection:
                result = await connection.execute(
                    update(history_leases)
                    .where(
                        history_leases.c.resource_key == resource_key,
                        history_leases.c.owner_id == owner_id,
                        history_leases.c.expires_at > now_text,
                    )
                    .values(expires_at=expires_at, updated_at=now_text)
                )
                renewed = result.rowcount == 1
        except SQLAlchemyError as error:
            raise self._database_error("history.lease_renew", error) from error
        return {"renewed": renewed, "resource_key": resource_key, "owner_id": owner_id}

    async def lease_release(self, resource_key: str, owner_id: str) -> dict[str, Any]:
        await self.initialize()
        try:
            async with self.engine.begin() as connection:
                result = await connection.execute(
                    delete(history_leases).where(
                        history_leases.c.resource_key == resource_key,
                        history_leases.c.owner_id == owner_id,
                    )
                )
        except SQLAlchemyError as error:
            raise self._database_error("history.lease_release", error) from error
        return {"released": result.rowcount == 1, "resource_key": resource_key}

    async def cleanup(self, *, ttl_days: int | None = None) -> dict[str, Any]:
        await self.initialize()
        deleted = 0
        cutoff = None
        if ttl_days is not None:
            cutoff = (
                datetime.now(UTC) - timedelta(days=ttl_days)
            ).isoformat().replace("+00:00", "Z")
        try:
            async with self.engine.begin() as connection:
                query = select(history_events.c.event_id).where(
                    history_events.c.status != OperationStatus.STARTED.value
                )
                if cutoff is not None:
                    query = query.where(history_events.c.completed_at < cutoff)
                event_ids = [row[0] for row in (await connection.execute(query)).all()]
                for event_id in event_ids:
                    await connection.execute(
                        delete(command_output_chunks).where(
                            command_output_chunks.c.event_id == event_id
                        )
                    )
                    await connection.execute(
                        delete(history_changes).where(history_changes.c.event_id == event_id)
                    )
                    await connection.execute(
                        delete(history_events).where(history_events.c.event_id == event_id)
                    )
                    deleted += 1
        except SQLAlchemyError as error:
            raise self._database_error("history.cleanup", error) from error
        return {"deleted_operations": deleted}

    async def close(self) -> None:
        await self.engine.dispose()

    async def _append_output(self, connection: Any, event: OperationEvent) -> bool:
        if event.operation_type != "command.execute" or event.result is None:
            return False
        added = False
        for stream in ("stdout", "stderr"):
            encoded = event.result.get(f"{stream}_base64")
            if not isinstance(encoded, str) or not encoded:
                continue
            data = base64.b64decode(encoded, validate=True)
            for index, offset in enumerate(range(0, len(data), self.config.output_chunk_bytes)):
                exists = (
                    await connection.execute(
                        select(command_output_chunks.c.id).where(
                            command_output_chunks.c.event_id == event.event_id,
                            command_output_chunks.c.stream == stream,
                            command_output_chunks.c.chunk_index == index,
                        )
                    )
                ).first()
                if exists is None:
                    await connection.execute(
                        insert(command_output_chunks).values(
                            event_id=event.event_id,
                            stream=stream,
                            chunk_index=index,
                            data=data[offset : offset + self.config.output_chunk_bytes],
                            created_at=self._time(event),
                        )
                    )
                    added = True
        return added

    async def _append_change(
        self,
        connection: Any,
        *,
        change_id: str,
        event: OperationEvent,
        change_type: str,
    ) -> None:
        exists = (
            await connection.execute(
                select(history_changes.c.seq).where(history_changes.c.change_id == change_id)
            )
        ).first()
        if exists is None:
            await connection.execute(
                insert(history_changes).values(
                    change_id=change_id,
                    event_id=event.event_id,
                    change_type=change_type,
                    changed_at=self._time(event),
                    schema_version=event.schema_version,
                )
            )

    def _started(self, event: OperationEvent) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "provider_key": self.identity.provider_key,
            "sandbox_id": self.identity.sandbox_id,
            "sandbox_instance_id": self.identity.sandbox_instance_id,
            "actor_type": event.actor.actor_type,
            "actor_id": event.actor.actor_id,
            "thread_id": event.actor.thread_id,
            "run_id": event.actor.run_id,
            "correlation_id": event.actor.correlation_id,
            "operation_type": event.operation_type,
            "status": event.status.value,
            "occurred_at": self._time(event),
            "request_json": event.request,
            "schema_version": event.schema_version,
        }

    @staticmethod
    def _time(event: OperationEvent) -> str:
        return event.occurred_at.isoformat().replace("+00:00", "Z")

    def _database_error(self, operation: str, error: Exception) -> HistoryDatabaseError:
        return HistoryDatabaseError(
            str(error),
            provider_name=self.identity.provider_name,
            provider_key=self.identity.provider_key,
            sandbox_id=self.identity.sandbox_id,
            sandbox_instance_id=self.identity.sandbox_instance_id,
            operation=operation,
            retryable=True,
        )
