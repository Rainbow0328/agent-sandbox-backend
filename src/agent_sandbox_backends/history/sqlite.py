from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.errors import HistoryDatabaseError
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.operations import OperationEvent
from agent_sandbox_backends.helper.source import history_helper
from agent_sandbox_backends.history.config import HistoryConfig
from agent_sandbox_backends.history.encoding import HistoryEventEncoder


class SQLiteHistoryStore:
    def __init__(
        self,
        database_path: str | Path,
        *,
        identity: SandboxRef,
        sdk_version: str,
        config: HistoryConfig | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self.identity = identity
        self.sdk_version = sdk_version
        self.config = config or HistoryConfig()
        self._encoder = HistoryEventEncoder(self.config)
        self._initialize_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._initialize_lock:
            if self._initialized:
                return
            try:
                await asyncio.to_thread(
                    history_helper.initialize,
                    self.database_path,
                    self._identity_payload(),
                    self.config.persisted_values(),
                    busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
                )
            except (history_helper.HelperError, sqlite3.Error, OSError) as error:
                raise self._database_error("history.initialize", error) from error
            self._initialized = True

    async def append(self, event: OperationEvent) -> None:
        await self.initialize()
        envelope = self._encoder.envelope(event)
        async with self._write_lock:
            try:
                await asyncio.to_thread(
                    history_helper.apply_batch,
                    self.database_path,
                    envelope,
                    busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
                )
            except (history_helper.HelperError, sqlite3.Error, OSError) as error:
                raise self._database_error(event.operation_type, error) from error

    async def close(self) -> None:
        return None

    async def append_output(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> None:
        if not chunks:
            return
        await self.initialize()
        envelope = self._encoder.output_envelope(event_id, chunks)
        async with self._write_lock:
            try:
                await asyncio.to_thread(
                    history_helper.apply_batch,
                    self.database_path,
                    envelope,
                    busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
                )
            except (history_helper.HelperError, sqlite3.Error, OSError) as error:
                raise self._database_error("history.output", error) from error

    async def query_changes(
        self,
        *,
        after_change_seq: int = 0,
        limit: int = 100,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        try:
            return await asyncio.to_thread(
                history_helper.query_changes,
                self.database_path,
                after_change_seq=after_change_seq,
                limit=limit,
                max_bytes=max_bytes or self.config.helper_query_max_bytes,
                busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
            )
        except (history_helper.HelperError, sqlite3.Error, OSError) as error:
            raise self._database_error("history.query", error) from error

    async def get_operation(self, event_id: str) -> dict[str, Any]:
        await self.initialize()
        try:
            return await asyncio.to_thread(
                history_helper.get_operation,
                self.database_path,
                event_id,
                busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
            )
        except (history_helper.HelperError, sqlite3.Error, OSError) as error:
            raise self._database_error("history.get_operation", error) from error

    async def acknowledge(self, consumer_id: str, seq: int) -> dict[str, Any]:
        await self.initialize()
        try:
            return await asyncio.to_thread(
                history_helper.acknowledge,
                self.database_path,
                consumer_id,
                seq,
                busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
            )
        except (history_helper.HelperError, sqlite3.Error, OSError) as error:
            raise self._database_error("history.ack", error) from error

    async def cleanup(self) -> dict[str, Any]:
        return await self._cleanup(if_due=False)

    async def cleanup_if_due(
        self,
        *,
        min_interval_seconds: int | None = None,
    ) -> dict[str, Any]:
        return await self._cleanup(
            if_due=True,
            min_interval_seconds=min_interval_seconds,
        )

    async def _cleanup(
        self,
        *,
        if_due: bool,
        min_interval_seconds: int | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        try:
            return await asyncio.to_thread(
                history_helper.cleanup,
                self.database_path,
                ttl_days=self.config.ttl_days,
                max_bytes=self.config.max_database_bytes,
                overflow_policy=self.config.overflow_policy.value,
                if_due=if_due,
                min_interval_seconds=(
                    self.config.cleanup_min_interval_seconds
                    if min_interval_seconds is None
                    else min_interval_seconds
                ),
                consumer_active_ttl_days=self.config.consumer_active_ttl_days,
                busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
            )
        except (history_helper.HelperError, sqlite3.Error, OSError) as error:
            raise self._database_error("history.cleanup", error) from error

    async def lease_acquire(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        return await self._lease_call(
            "history.lease_acquire",
            history_helper.lease_acquire,
            resource_key,
            owner_id,
            ttl_ms,
        )

    async def lease_renew(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        return await self._lease_call(
            "history.lease_renew",
            history_helper.lease_renew,
            resource_key,
            owner_id,
            ttl_ms,
        )

    async def lease_release(self, resource_key: str, owner_id: str) -> dict[str, Any]:
        await self.initialize()
        try:
            return await asyncio.to_thread(
                history_helper.lease_release,
                self.database_path,
                resource_key,
                owner_id,
                busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
            )
        except (history_helper.HelperError, sqlite3.Error, OSError) as error:
            raise self._database_error("history.lease_release", error) from error

    async def _lease_call(
        self,
        operation: str,
        call: Any,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        await self.initialize()
        try:
            result: dict[str, Any] = await asyncio.to_thread(
                call,
                self.database_path,
                resource_key,
                owner_id,
                ttl_ms,
                busy_timeout_ms=self.config.sqlite_busy_timeout_ms,
            )
            return result
        except (history_helper.HelperError, sqlite3.Error, OSError) as error:
            raise self._database_error(operation, error) from error

    def _identity_payload(self) -> dict[str, Any]:
        return {
            "provider_key": self.identity.provider_key,
            "sandbox_id": self.identity.sandbox_id,
            "sandbox_instance_id": self.identity.sandbox_instance_id,
            "sdk_version": self.sdk_version,
            "schema_version": history_helper.SCHEMA_VERSION,
        }

    def _database_error(self, operation: str, error: Exception) -> HistoryDatabaseError:
        return HistoryDatabaseError(
            str(error),
            provider_name=self.identity.provider_name,
            provider_key=self.identity.provider_key,
            sandbox_id=self.identity.sandbox_id,
            sandbox_instance_id=self.identity.sandbox_instance_id,
            operation=operation,
            retryable=isinstance(error, sqlite3.OperationalError),
        )
