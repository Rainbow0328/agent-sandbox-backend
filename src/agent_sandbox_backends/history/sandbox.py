from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.errors import HistoryTransportError
from agent_sandbox_backends.domain.operations import OperationEvent, OperationStatus
from agent_sandbox_backends.history.bootstrap_outbox import BootstrapOutbox
from agent_sandbox_backends.history.config import HistoryConfig
from agent_sandbox_backends.history.encoding import HistoryEventEncoder
from agent_sandbox_backends.ports.history_transport import HistoryHelperTransport


class SandboxHistoryStore:
    def __init__(
        self,
        transport: HistoryHelperTransport,
        *,
        sdk_version: str,
        config: HistoryConfig | None = None,
    ) -> None:
        self.transport = transport
        self.identity = transport.identity
        self.sdk_version = sdk_version
        self.config = config or HistoryConfig()
        self._encoder = HistoryEventEncoder(self.config)
        self._initialize_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._initialized = False

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
            await self._with_retry(
                lambda: self.transport.invoke(
                    "apply",
                    payload=envelope,
                    payload_flag="--batch-file",
                )
            )

    async def initialize(self) -> dict[str, Any]:
        if self._initialized:
            return await self.health()
        async with self._initialize_lock:
            if not self._initialized:
                result = await self._with_retry(
                    lambda: self.transport.ensure_installed(
                        sdk_version=self.sdk_version,
                        config=self.config,
                    )
                )
                self._initialized = True
                return result
        return await self.health()

    async def append(self, event: OperationEvent) -> None:
        await self.initialize()
        envelope = self._encoder.envelope(event)
        async with self._write_lock:
            await self._with_retry(
                lambda: self.transport.invoke(
                    "apply",
                    payload=envelope,
                    payload_flag="--batch-file",
                )
            )

    async def import_outbox(self, outbox: BootstrapOutbox) -> int:
        events = await outbox.events()
        grouped: dict[str, list[OperationEvent]] = {}
        for event in events:
            grouped.setdefault(event.event_id, []).append(event)

        imported = 0
        for event_id, operation_events in grouped.items():
            ordered = sorted(
                operation_events,
                key=lambda event: (
                    event.occurred_at,
                    0 if event.status == OperationStatus.STARTED else 1,
                ),
            )
            for event in ordered:
                await self.append(event)
                await outbox.mark_imported(event)
                imported += 1
            await outbox.remove(event_id)
        return imported

    async def health(self) -> dict[str, Any]:
        return await self._with_retry(lambda: self.transport.invoke("health"))

    async def get_identity(self) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(lambda: self.transport.invoke("identity-get"))

    async def query_changes(
        self,
        *,
        after_change_seq: int = 0,
        limit: int = 100,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                "query",
                arguments=(
                    "--after-change-seq",
                    str(after_change_seq),
                    "--limit",
                    str(limit),
                    "--max-bytes",
                    str(max_bytes or self.config.helper_query_max_bytes),
                ),
            )
        )

    async def get_operation(self, event_id: str) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                "get-operation",
                arguments=("--event-id", event_id),
            )
        )

    async def get_output(
        self,
        event_id: str,
        stream: str,
        *,
        after_chunk: int = -1,
    ) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                "get-output",
                arguments=(
                    "--event-id",
                    event_id,
                    "--stream",
                    stream,
                    "--after-chunk",
                    str(after_chunk),
                ),
            )
        )

    async def acknowledge(self, consumer_id: str, seq: int) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                "ack",
                arguments=("--consumer-id", consumer_id, "--seq", str(seq)),
            )
        )

    async def config_get(self) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(lambda: self.transport.invoke("config-get"))

    async def config_set(
        self,
        changes: dict[str, Any],
        *,
        expected_revision: int,
        updated_by: str,
    ) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                "config-set",
                arguments=("--expected-revision", str(expected_revision)),
                payload={"changes": changes, "updated_by": updated_by},
                payload_flag="--config-file",
            )
        )

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
        arguments = [
            "--max-bytes",
            str(self.config.max_database_bytes),
            "--overflow-policy",
            self.config.overflow_policy.value,
            "--min-interval-seconds",
            str(
                self.config.cleanup_min_interval_seconds
                if min_interval_seconds is None
                else min_interval_seconds
            ),
            "--consumer-active-ttl-days",
            str(self.config.consumer_active_ttl_days),
        ]
        if if_due:
            arguments.append("--if-due")
        if self.config.ttl_days is not None:
            arguments.extend(("--ttl-days", str(self.config.ttl_days)))
        return await self._with_retry(
            lambda: self.transport.invoke("cleanup", arguments=tuple(arguments))
        )

    async def lease_acquire(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        return await self._lease_command("lease-acquire", resource_key, owner_id, ttl_ms)

    async def lease_renew(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        return await self._lease_command("lease-renew", resource_key, owner_id, ttl_ms)

    async def lease_release(self, resource_key: str, owner_id: str) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                "lease-release",
                arguments=("--resource-key", resource_key, "--owner-id", owner_id),
            )
        )

    async def _lease_command(
        self,
        command: str,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]:
        await self.initialize()
        return await self._with_retry(
            lambda: self.transport.invoke(
                command,
                arguments=(
                    "--resource-key",
                    resource_key,
                    "--owner-id",
                    owner_id,
                    "--ttl-ms",
                    str(ttl_ms),
                ),
            )
        )

    async def _with_retry(
        self,
        call: Callable[[], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        attempts = self.config.sync_retry_attempts + 1
        delay_ms = self.config.sync_retry_base_delay_ms
        for attempt in range(attempts):
            try:
                result: dict[str, Any] = await call()
                return result
            except HistoryTransportError as error:
                if not error.retryable or attempt + 1 >= attempts:
                    raise
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000)
                delay_ms = min(
                    max(delay_ms * 2, 1),
                    self.config.sync_retry_max_delay_ms,
                )
        raise RuntimeError("History retry loop exhausted")
