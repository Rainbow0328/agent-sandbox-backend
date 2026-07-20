from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TypeVar

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.concurrency.activity import OperationActivityGate
from agent_sandbox_backends.config.retry import RetryConfig
from agent_sandbox_backends.domain.context import get_actor_context
from agent_sandbox_backends.domain.errors import (
    HistoryDatabaseError,
    HistoryTransportError,
    SandboxBackendError,
)
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.operations import OperationEvent, OperationStatus
from agent_sandbox_backends.history.config import HistoryConsistency
from agent_sandbox_backends.ports.history_store import HistoryStore

ResultT = TypeVar("ResultT")


class OperationPipeline:
    def __init__(
        self,
        history_store: HistoryStore,
        *,
        retry: RetryConfig | None = None,
        activity_gate: OperationActivityGate | None = None,
        consistency: HistoryConsistency = HistoryConsistency.STRICT_START,
    ) -> None:
        self._history_store = history_store
        self._retry = retry or RetryConfig()
        self._activity_gate = activity_gate
        self._consistency = consistency

    async def run(
        self,
        operation_type: str,
        call: Callable[[], Awaitable[ResultT]],
        *,
        sandbox_ref: SandboxRef | None = None,
        request: dict[str, object] | None = None,
        result_encoder: Callable[[ResultT], dict[str, object]] | None = None,
        status_encoder: Callable[[ResultT], OperationStatus] | None = None,
        retryable: bool = False,
        track_activity: bool = True,
        operation_id: str | None = None,
    ) -> ResultT:
        if self._activity_gate is not None and track_activity:
            async with self._activity_gate.operation():
                return await self._run(
                    operation_type,
                    call,
                    sandbox_ref=sandbox_ref,
                    request=request,
                    result_encoder=result_encoder,
                    status_encoder=status_encoder,
                    retryable=retryable,
                    operation_id=operation_id,
                )
        return await self._run(
            operation_type,
            call,
            sandbox_ref=sandbox_ref,
            request=request,
            result_encoder=result_encoder,
            status_encoder=status_encoder,
            retryable=retryable,
            operation_id=operation_id,
        )

    async def _run(
        self,
        operation_type: str,
        call: Callable[[], Awaitable[ResultT]],
        *,
        sandbox_ref: SandboxRef | None,
        request: dict[str, object] | None,
        result_encoder: Callable[[ResultT], dict[str, object]] | None,
        status_encoder: Callable[[ResultT], OperationStatus] | None,
        retryable: bool,
        operation_id: str | None,
    ) -> ResultT:
        resolved_operation_id = operation_id or str(uuid7())
        actor = get_actor_context()
        started_at = time.perf_counter_ns()
        started = OperationEvent(
            event_id=resolved_operation_id,
            occurred_at=datetime.now(UTC),
            operation_type=operation_type,
            status=OperationStatus.STARTED,
            actor=actor,
            sandbox_ref=sandbox_ref,
            request=request or {},
        )
        try:
            await self._history_store.append(started)
        except (HistoryDatabaseError, HistoryTransportError):
            if self._consistency == HistoryConsistency.STRICT_START:
                raise

        try:
            result = await self._run_with_retry(call, retryable=retryable)
        except asyncio.CancelledError:
            await self._append_terminal_safely(
                started,
                OperationStatus.CANCELLED,
                started_at,
                error_code="cancelled",
            )
            raise
        except TimeoutError as error:
            await self._append_terminal_safely(
                started,
                OperationStatus.TIMEOUT,
                started_at,
                result={"error": str(error), "error_type": type(error).__name__},
                error_code="timeout",
            )
            raise
        except SandboxBackendError as error:
            error.operation = error.operation or operation_type
            error.operation_id = error.operation_id or resolved_operation_id
            error.actor_id = error.actor_id or actor.actor_id
            error.correlation_id = error.correlation_id or actor.correlation_id
            await self._append_terminal_safely(
                started,
                OperationStatus.FAILED,
                started_at,
                result=self._error_result(error),
                error_code=error.code,
            )
            raise
        except Exception as error:
            await self._append_terminal_safely(
                started,
                OperationStatus.FAILED,
                started_at,
                result={"error": str(error), "error_type": type(error).__name__},
                error_code=type(error).__name__,
            )
            raise

        encoded = result_encoder(result) if result_encoder is not None else None
        terminal_status = (
            status_encoder(result)
            if status_encoder is not None
            else OperationStatus.SUCCEEDED
        )
        try:
            await self._append_terminal(
                started,
                terminal_status,
                started_at,
                result=encoded,
                error_code=(
                    None
                    if terminal_status == OperationStatus.SUCCEEDED
                    else terminal_status.value
                ),
            )
        except (HistoryDatabaseError, HistoryTransportError):
            pass
        return result

    @staticmethod
    def _error_result(error: SandboxBackendError) -> dict[str, object]:
        return {
            "error": error.message,
            "error_type": type(error).__name__,
            "provider_name": error.provider_name,
            "provider_key": error.provider_key,
            "sandbox_id": error.sandbox_id,
            "sandbox_instance_id": error.sandbox_instance_id,
            "provider_error_code": error.provider_error_code,
            "provider_request_id": error.provider_request_id,
            "retryable": error.retryable,
            "details": error.details,
        }

    async def _run_with_retry(
        self,
        call: Callable[[], Awaitable[ResultT]],
        *,
        retryable: bool,
    ) -> ResultT:
        started_at = time.monotonic()
        delay_ms = self._retry.base_delay_ms
        for attempt in range(self._retry.max_attempts):
            try:
                return await call()
            except asyncio.CancelledError:
                raise
            except SandboxBackendError as error:
                if (
                    not retryable
                    or not error.retryable
                    or attempt + 1 >= self._retry.max_attempts
                ):
                    raise
                delay_seconds = self._jittered_delay(delay_ms)
                deadline = self._retry.total_deadline_seconds
                elapsed = time.monotonic() - started_at
                if deadline is not None and elapsed + delay_seconds >= deadline:
                    raise
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                delay_ms = min(max(delay_ms * 2, 1), self._retry.max_delay_ms)
        raise RuntimeError("Retry loop exhausted")

    def _jittered_delay(self, delay_ms: int) -> float:
        if delay_ms == 0:
            return 0
        spread = delay_ms * self._retry.jitter_ratio
        jittered = random.uniform(delay_ms - spread, delay_ms + spread)
        return max(0, jittered) / 1000

    async def _append_terminal_safely(
        self,
        started: OperationEvent,
        status: OperationStatus,
        started_at: int,
        *,
        result: dict[str, object] | None = None,
        error_code: str | None = None,
    ) -> None:
        try:
            await self._append_terminal(
                started,
                status,
                started_at,
                result=result,
                error_code=error_code,
            )
        except (HistoryDatabaseError, HistoryTransportError):
            pass

    async def _append_terminal(
        self,
        started: OperationEvent,
        status: OperationStatus,
        started_at: int,
        *,
        result: dict[str, object] | None = None,
        error_code: str | None = None,
    ) -> None:
        duration_ms = max(0, (time.perf_counter_ns() - started_at) // 1_000_000)
        await self._history_store.append(
            OperationEvent(
                event_id=started.event_id,
                occurred_at=datetime.now(UTC),
                operation_type=started.operation_type,
                status=status,
                actor=started.actor,
                sandbox_ref=started.sandbox_ref,
                request=started.request,
                result=result,
                duration_ms=duration_ms,
                error_code=error_code,
            )
        )
