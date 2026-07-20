from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager
from pathlib import PurePosixPath

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.application.command_handle import CommandHandle
from agent_sandbox_backends.application.operation_pipeline import OperationPipeline
from agent_sandbox_backends.application.output_writer import CommandHistoryOutputWriter
from agent_sandbox_backends.concurrency import KeyedRWLock, OperationActivityGate, QueueLimiter
from agent_sandbox_backends.config.concurrency import ConcurrencyConfig
from agent_sandbox_backends.config.models import CleanupPolicy
from agent_sandbox_backends.config.retry import RetryConfig
from agent_sandbox_backends.config.upload import UploadConfig
from agent_sandbox_backends.domain.capabilities import Capabilities
from agent_sandbox_backends.domain.commands import CommandState, ExecRequest, ExecResult
from agent_sandbox_backends.domain.context import ActorContext, actor_context
from agent_sandbox_backends.domain.errors import (
    BackendCloseError,
    CommandQueueTimeoutError,
    LockAcquisitionTimeoutError,
    SandboxBackendError,
    SandboxDeletingError,
    SandboxNotFoundError,
)
from agent_sandbox_backends.domain.files import FileEntry, WriteFileRequest, WriteFileResult
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.operations import OperationEvent, OperationStatus
from agent_sandbox_backends.domain.sandbox import SandboxInfo, SandboxState
from agent_sandbox_backends.domain.uploads import UploadResult, UploadSpec
from agent_sandbox_backends.history.config import HistoryConfig, HistoryConsistency
from agent_sandbox_backends.history.memory import MemoryHistoryStore
from agent_sandbox_backends.history.sandbox import SandboxHistoryStore
from agent_sandbox_backends.ports.history_store import HistoryStore
from agent_sandbox_backends.ports.provider import SandboxProvider
from agent_sandbox_backends.upload.service import UploadService


def _command_operation_status(result: ExecResult) -> OperationStatus:
    return {
        CommandState.SUCCEEDED: OperationStatus.SUCCEEDED,
        CommandState.FAILED: OperationStatus.FAILED,
        CommandState.CANCELLED: OperationStatus.CANCELLED,
        CommandState.TIMEOUT: OperationStatus.TIMEOUT,
        CommandState.UNKNOWN: OperationStatus.UNKNOWN,
        CommandState.QUEUED: OperationStatus.UNKNOWN,
        CommandState.RUNNING: OperationStatus.UNKNOWN,
    }[result.state]


class SandboxBackend:
    def __init__(
        self,
        *,
        provider: SandboxProvider,
        ref: SandboxRef,
        history_store: HistoryStore,
        cleanup: CleanupPolicy,
        concurrency: ConcurrencyConfig | None = None,
        retry: RetryConfig | None = None,
        history_consistency: HistoryConsistency = HistoryConsistency.STRICT_START,
        history_config: HistoryConfig | None = None,
        cleanup_ttl_seconds: float | None = None,
        owns_provider: bool = False,
        owns_history_store: bool = False,
        close_coordinator: Callable[
            [SandboxBackend, Callable[[], Awaitable[None]]], Awaitable[None]
        ]
        | None = None,
    ) -> None:
        self.provider = provider
        self.ref = ref
        self.history_store = history_store
        self.cleanup = cleanup
        self._owns_provider = owns_provider
        self._owns_history_store = owns_history_store
        self._history_config = history_config or HistoryConfig()
        self._cleanup_ttl_seconds = cleanup_ttl_seconds
        self._ttl_cleanup_task: asyncio.Task[None] | None = None
        self._close_coordinator = close_coordinator
        self.concurrency = concurrency or ConcurrencyConfig()
        self._activity = OperationActivityGate()
        self._pipeline = OperationPipeline(
            history_store,
            retry=retry,
            activity_gate=self._activity,
            consistency=history_consistency,
        )
        self._file_locks = KeyedRWLock()
        self._file_reads = QueueLimiter(
            self.concurrency.max_parallel_file_reads,
            timeout_seconds=self.concurrency.file_queue_timeout,
            error_type=LockAcquisitionTimeoutError,
            resource_name="file read slot",
        )
        self._file_writes = QueueLimiter(
            self.concurrency.max_parallel_file_writes,
            timeout_seconds=self.concurrency.file_queue_timeout,
            error_type=LockAcquisitionTimeoutError,
            resource_name="file write slot",
        )
        self._commands = QueueLimiter(
            self.concurrency.max_parallel_commands,
            timeout_seconds=self.concurrency.command_queue_timeout,
            error_type=CommandQueueTimeoutError,
            resource_name="command slot",
        )
        self._uploads = QueueLimiter(
            self.concurrency.max_parallel_uploads,
            timeout_seconds=self.concurrency.upload_queue_timeout,
            error_type=LockAcquisitionTimeoutError,
            resource_name="upload slot",
        )
        self._lifecycle = QueueLimiter(
            1,
            timeout_seconds=self.concurrency.lifecycle_lock_timeout,
            error_type=LockAcquisitionTimeoutError,
            resource_name="sandbox lifecycle lock",
        )
        self._close_lock = asyncio.Lock()
        self._closed = False
        self._deleting = False

    def agent_context(
        self,
        *,
        agent_id: str,
        thread_id: str | None = None,
        run_id: str | None = None,
        correlation_id: str | None = None,
    ) -> AbstractContextManager[ActorContext]:
        return actor_context(
            actor_type="agent",
            actor_id=agent_id,
            thread_id=thread_id,
            run_id=run_id,
            correlation_id=correlation_id,
        )

    async def info(self) -> SandboxInfo:
        self._ensure_open()
        return await self._pipeline.run(
            "sandbox.get",
            lambda: self.provider.get(self.ref),
            sandbox_ref=self.ref,
            result_encoder=lambda info: {"state": info.state.value},
        )

    async def capabilities(self) -> Capabilities:
        self._ensure_open()
        return await self._pipeline.run(
            "sandbox.capabilities",
            lambda: self.provider.capabilities(self.ref),
            sandbox_ref=self.ref,
        )

    async def pause(self) -> SandboxInfo:
        self._ensure_open()
        async with self._lifecycle.slot():
            return await self._pipeline.run(
                "sandbox.pause",
                lambda: self._lifecycle_with_reconciliation(
                    self.provider.pause,
                    SandboxState.PAUSED,
                ),
                sandbox_ref=self.ref,
                result_encoder=lambda info: {"state": info.state.value},
            )

    async def resume(self) -> SandboxInfo:
        self._ensure_open()
        async with self._lifecycle.slot():
            return await self._pipeline.run(
                "sandbox.resume",
                lambda: self._lifecycle_with_reconciliation(
                    self.provider.resume,
                    SandboxState.RUNNING,
                ),
                sandbox_ref=self.ref,
                result_encoder=lambda info: {"state": info.state.value},
            )

    async def list_files(self, path: str = ".") -> list[FileEntry]:
        self._ensure_open()
        key = self._path_key(path)
        async with self._file_reads.slot(), self._file_locks.read(
            key,
            timeout_seconds=self.concurrency.file_queue_timeout,
        ):
            return await self._pipeline.run(
                "file.list",
                lambda: self.provider.list_files(self.ref, path),
                sandbox_ref=self.ref,
                request={"path": path},
                result_encoder=lambda entries: {"count": len(entries)},
                retryable=True,
            )

    async def read_file(self, path: str) -> bytes:
        self._ensure_open()
        key = self._path_key(path)
        async with self._file_reads.slot(), self._file_locks.read(
            key,
            timeout_seconds=self.concurrency.file_queue_timeout,
        ):
            return await self._pipeline.run(
                "file.read",
                lambda: self.provider.read_file(self.ref, path),
                sandbox_ref=self.ref,
                request={"path": path},
                result_encoder=lambda content: {"size": len(content)},
                retryable=True,
            )

    async def write_file(
        self,
        path: str,
        content: bytes,
        *,
        expected_hash: str | None = None,
    ) -> WriteFileResult:
        self._ensure_open()
        request = WriteFileRequest(
            path=path,
            content=content,
            expected_hash=expected_hash,
        )
        key = self._path_key(path)
        async with self._file_writes.slot(), self._file_locks.write(
            key,
            timeout_seconds=self.concurrency.file_queue_timeout,
        ):
            return await self._pipeline.run(
                "file.write",
                lambda: self.provider.write_file(self.ref, request),
                sandbox_ref=self.ref,
                request={
                    "path": path,
                    "size": len(content),
                    "expected_hash": expected_hash,
                },
                result_encoder=lambda result: {
                    "path": result.entry.path,
                    "size": result.entry.size,
                    "content_hash": result.entry.content_hash,
                },
            )

    async def delete_file(self, path: str) -> None:
        self._ensure_open()
        key = self._path_key(path)
        async with self._file_writes.slot(), self._file_locks.write(
            key,
            timeout_seconds=self.concurrency.file_queue_timeout,
        ):
            await self._pipeline.run(
                "file.delete",
                lambda: self.provider.delete_file(self.ref, path),
                sandbox_ref=self.ref,
                request={"path": path},
            )

    async def upload_local(
        self,
        spec: UploadSpec,
        *,
        config: UploadConfig | None = None,
    ) -> UploadResult:
        self._ensure_open()
        service = UploadService(self, config=config)
        async with self._uploads.slot(), self._file_locks.write(
            f"upload:{self._path_key(spec.target)}",
            timeout_seconds=self.concurrency.upload_queue_timeout,
        ):
            return await self._pipeline.run(
                "file.upload",
                lambda: service.upload(spec),
                sandbox_ref=self.ref,
                request={"target": spec.target, "conflict": spec.conflict.value},
                result_encoder=lambda result: {
                    "upload_id": result.upload_id,
                    "manifest_hash": result.manifest_hash,
                    "uploaded_files": result.uploaded_files,
                    "skipped_files": result.skipped_files,
                    "failed_files": result.failed_files,
                    "atomic": result.atomic,
                },
            )

    async def execute(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
    ) -> ExecResult:
        self._ensure_open()
        request = ExecRequest(
            command=command,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
        )
        async with self._commands.slot():
            operation_id = str(uuid7())
            output_writer = CommandHistoryOutputWriter(
                self.history_store,
                event_id=operation_id,
                config=self._history_config,
            )

            async def execute_streaming() -> ExecResult:
                try:
                    return await self.provider.execute_stream(
                        self.ref,
                        request,
                        output_writer.write,
                    )
                finally:
                    await output_writer.close()

            return await self._pipeline.run(
                "command.execute",
                execute_streaming,
                sandbox_ref=self.ref,
                request={
                    "command": command,
                    "cwd": cwd,
                    "timeout_seconds": timeout_seconds,
                },
                result_encoder=lambda result: {
                    "command_id": result.command_id,
                    "state": result.state.value,
                    "exit_code": result.exit_code,
                    "stdout_bytes": len(result.stdout),
                    "stderr_bytes": len(result.stderr),
                    "stdout_total_bytes": result.stdout_total_bytes,
                    "stderr_total_bytes": result.stderr_total_bytes,
                    "stdout_truncated": result.stdout_truncated,
                    "stderr_truncated": result.stderr_truncated,
                    "stdout_base64": (
                        base64.b64encode(result.stdout).decode("ascii")
                        if output_writer.use_terminal_fallback
                        else ""
                    ),
                    "stderr_base64": (
                        base64.b64encode(result.stderr).decode("ascii")
                        if output_writer.use_terminal_fallback
                        else ""
                    ),
                    "output_complete": output_writer.output_complete,
                    "history_storage_state": output_writer.storage_state,
                    "history_storage_error": output_writer.failure_reason,
                },
                status_encoder=_command_operation_status,
                operation_id=operation_id,
            )

    def start_command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout_seconds: float | None = None,
    ) -> CommandHandle:
        self._ensure_open()
        task = asyncio.create_task(
            self.execute(command, cwd=cwd, timeout_seconds=timeout_seconds),
            name=f"sandbox-command:{self.ref.sandbox_id}",
        )
        return CommandHandle(task)

    async def history_events(self) -> tuple[OperationEvent, ...]:
        if isinstance(self.history_store, MemoryHistoryStore):
            return await self.history_store.events()
        raise TypeError("The configured history store does not expose in-memory events")

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            errors: list[BaseException] = []
            try:
                await self._activity.begin_close(
                    timeout_seconds=self.concurrency.lifecycle_lock_timeout
                )
            except BaseException as error:
                errors.append(error)
            if self.cleanup == CleanupPolicy.TTL:
                self._ttl_cleanup_task = asyncio.create_task(
                    self._delete_after_ttl(),
                    name=f"sandbox-ttl-cleanup:{self.ref.sandbox_id}",
                )
                self._closed = True
                if errors:
                    self._raise_close_errors(errors)
                return
            try:
                if self._close_coordinator is not None:
                    await self._close_coordinator(self, self._delete_owned_sandbox)
                elif self.cleanup == CleanupPolicy.ON_CLOSE:
                    await self._delete_owned_sandbox()
            except BaseException as error:
                errors.append(error)
            await self._close_owned_resources(errors)
            if errors:
                try:
                    await self._activity.reopen()
                except BaseException as error:
                    errors.append(error)
                self._raise_close_errors(errors)
            self._closed = True

    async def _delete_after_ttl(self) -> None:
        delay = self._cleanup_ttl_seconds
        if delay is None:
            return
        try:
            await asyncio.sleep(delay)
            await self._delete_owned_sandbox()
        finally:
            errors: list[BaseException] = []
            await self._close_owned_resources(errors)
            if errors:
                self._raise_close_errors(errors)

    async def _close_owned_resources(self, errors: list[BaseException]) -> None:
        if self._owns_history_store:
            try:
                await self.history_store.close()
            except BaseException as error:
                errors.append(error)
        if self._owns_provider:
            try:
                await self.provider.close()
            except BaseException as error:
                errors.append(error)

    @staticmethod
    def _raise_close_errors(errors: list[BaseException]) -> None:
        if len(errors) == 1:
            raise errors[0]
        raise BackendCloseError(errors)

    async def _delete_owned_sandbox(self) -> None:
        self._deleting = True
        try:
            async with self._lifecycle.slot():
                if isinstance(self.history_store, SandboxHistoryStore):
                    # Sandbox 内的历史数据库会随 Sandbox 一起删除, 因此删除成功后
                    # 已经没有存储位置可以追加 sandbox.delete 的终态记录。
                    await self._delete_with_reconciliation()
                else:
                    await self._pipeline.run(
                        "sandbox.delete",
                        self._delete_with_reconciliation,
                        sandbox_ref=self.ref,
                        track_activity=False,
                    )
        except Exception:
            self._deleting = False
            raise

    async def _lifecycle_with_reconciliation(
        self,
        operation: Callable[[SandboxRef], Awaitable[SandboxInfo]],
        expected_state: SandboxState,
    ) -> SandboxInfo:
        try:
            return await operation(self.ref)
        except SandboxBackendError:
            actual = await self.provider.get(self.ref)
            if actual.state == expected_state:
                return actual
            raise

    async def _delete_with_reconciliation(self) -> None:
        try:
            await self.provider.delete(self.ref)
        except SandboxBackendError:
            try:
                await self.provider.get(self.ref)
            except SandboxNotFoundError:
                return
            raise

    async def __aenter__(self) -> SandboxBackend:
        self._ensure_open()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SandboxBackend is closed")
        if self._deleting:
            raise SandboxDeletingError("Sandbox deletion is in progress")

    @staticmethod
    def _path_key(path: str) -> str:
        candidate = PurePosixPath(path)
        if ".." in candidate.parts:
            raise ValueError("Parent path traversal is not allowed")
        return str(candidate)
