from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Protocol, cast

from opensandbox import Sandbox
from opensandbox.config.connection import ConnectionConfig
from opensandbox.exceptions import (
    InvalidArgumentException,
    SandboxException,
    SandboxInternalException,
    SandboxReadyTimeoutException,
    SandboxUnhealthyException,
)
from opensandbox.manager import SandboxManager
from opensandbox.models.execd import ExecutionHandlers, RunCommandOpts
from opensandbox.models.filesystem import DirectoryListEntry, EntryInfo
from opensandbox.models.sandboxes import SandboxFilter

from agent_sandbox_backends._internal.bounded_output import BoundedHeadTailBuffer
from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.config.command import CommandResultConfig
from agent_sandbox_backends.domain.capabilities import (
    Capabilities,
    native_capability,
    unavailable_capability,
)
from agent_sandbox_backends.domain.commands import (
    CommandState,
    CommandStream,
    ExecRequest,
    ExecResult,
)
from agent_sandbox_backends.domain.errors import (
    ConcurrentModificationError,
    ProviderError,
    SandboxBackendError,
    SandboxNotFoundError,
    SandboxStateError,
)
from agent_sandbox_backends.domain.errors import (
    FileNotFoundError as BackendFileNotFoundError,
)
from agent_sandbox_backends.domain.files import (
    FileEntry,
    FileKind,
    WriteFileRequest,
    WriteFileResult,
)
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.sandbox import CreateSandboxRequest, SandboxInfo, SandboxState
from agent_sandbox_backends.ports.provider import SandboxProvider


class _ExecutionInitMessage(Protocol):
    id: object


class _ExecutionOutputMessage(Protocol):
    text: object


class OpenSandboxProvider(SandboxProvider):
    provider_name = "opensandbox"

    def __init__(
        self,
        *,
        provider_key: str = "opensandbox-default",
        connection_config: ConnectionConfig | None = None,
        api_key: str | None = None,
        domain: str | None = None,
        protocol: str = "http",
        request_timeout_seconds: float = 30,
        ready_timeout_seconds: float = 30,
        debug: bool = False,
        headers: dict[str, str] | None = None,
        use_server_proxy: bool = False,
        sandbox_class: Any = Sandbox,
        manager_class: Any = SandboxManager,
        command_result_config: CommandResultConfig | None = None,
    ) -> None:
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be greater than zero")
        if ready_timeout_seconds <= 0:
            raise ValueError("ready_timeout_seconds must be greater than zero")
        self.provider_key = provider_key
        self._connection_config = connection_config or ConnectionConfig(
            api_key=api_key,
            domain=domain,
            protocol=protocol,
            request_timeout=timedelta(seconds=request_timeout_seconds),
            debug=debug,
            headers=headers or {},
            use_server_proxy=use_server_proxy,
        )
        self._sandbox_class = sandbox_class
        self._manager_class = manager_class
        self._command_result_config = command_result_config or CommandResultConfig()
        self._ready_timeout = timedelta(seconds=ready_timeout_seconds)
        self._sandboxes: dict[tuple[str, str, str], Any] = {}
        self._create_keys: dict[str, SandboxRef] = {}
        self._manager: Any | None = None
        self._lock = asyncio.Lock()

    async def create(self, request: CreateSandboxRequest) -> SandboxRef:
        async with self._lock:
            if request.idempotency_key:
                existing = self._create_keys.get(request.idempotency_key)
                if existing is not None:
                    return existing
            try:
                metadata = dict(request.metadata)
                metadata.setdefault("sandbox_instance_id", str(uuid7()))
                create_options: dict[str, Any] = {
                    "env": request.env,
                    "metadata": metadata,
                    "connection_config": self._connection_config,
                    "ready_timeout": self._ready_timeout,
                }
                if request.sandbox_ttl_seconds is not None:
                    create_options["timeout"] = timedelta(
                        seconds=request.sandbox_ttl_seconds
                    )
                sandbox = await self._sandbox_class.create(
                    request.image,
                    **create_options,
                )
                info = await sandbox.get_info()
            except Exception as error:
                raise self._normalize_error("sandbox.create", error) from error
            ref = self._ref_from_info(info)
            self._sandboxes[self._cache_key(ref)] = sandbox
            if request.idempotency_key:
                self._create_keys[request.idempotency_key] = ref
            return ref

    async def get(self, ref: SandboxRef) -> SandboxInfo:
        sandbox = await self._get_sandbox(ref)
        try:
            return self._info_from_sdk(await sandbox.get_info(), ref)
        except Exception as error:
            raise self._normalize_error("sandbox.get", error, ref=ref) from error

    async def list(self) -> list[SandboxInfo]:
        manager = await self._get_manager()
        try:
            result = await manager.list_sandbox_infos(SandboxFilter(page_size=100, page=1))
            infos = list(result.sandbox_infos)
            page = result.pagination
            while page.has_next_page:
                page_number = page.page + 1
                result = await manager.list_sandbox_infos(
                    SandboxFilter(page_size=100, page=page_number)
                )
                infos.extend(result.sandbox_infos)
                page = result.pagination
            return [self._info_from_sdk(info) for info in infos]
        except Exception as error:
            raise self._normalize_error("sandbox.list", error) from error

    async def pause(self, ref: SandboxRef) -> SandboxInfo:
        sandbox = await self._get_sandbox(ref)
        try:
            await sandbox.pause()
            return await self.get(ref)
        except Exception as error:
            raise self._normalize_error("sandbox.pause", error, ref=ref) from error

    async def resume(self, ref: SandboxRef) -> SandboxInfo:
        self._validate_ref(ref)
        try:
            sandbox = await self._sandbox_class.resume(
                ref.sandbox_id,
                connection_config=self._connection_config,
            )
            await self._validate_remote_identity(sandbox, ref)
            self._sandboxes[self._cache_key(ref)] = sandbox
            return await self.get(ref)
        except Exception as error:
            raise self._normalize_error("sandbox.resume", error, ref=ref) from error

    async def delete(self, ref: SandboxRef) -> None:
        self._validate_ref(ref)
        cache_key = self._cache_key(ref)
        sandbox = self._sandboxes.get(cache_key)
        try:
            if sandbox is None:
                sandbox = await self._sandbox_class.connect(
                    ref.sandbox_id,
                    connection_config=self._connection_config,
                    skip_health_check=True,
                )
                await self._validate_remote_identity(sandbox, ref)
            await sandbox.kill()
            await sandbox.close()
        except Exception as error:
            normalized = self._normalize_error("sandbox.delete", error, ref=ref)
            if normalized.code == "sandbox_not_found":
                self._sandboxes.pop(cache_key, None)
                return
            raise normalized from error
        finally:
            self._sandboxes.pop(cache_key, None)

    async def renew_expiration(
        self, ref: SandboxRef, timeout_seconds: float
    ) -> SandboxInfo:
        sandbox = await self._get_sandbox(ref)
        try:
            await sandbox.renew(timedelta(seconds=timeout_seconds))
            return await self.get(ref)
        except Exception as error:
            raise self._normalize_error(
                "sandbox.renew_expiration", error, ref=ref
            ) from error

    async def capabilities(self, ref: SandboxRef | None = None) -> Capabilities:
        if ref is not None:
            await self._get_sandbox(ref)
        return Capabilities(
            filesystem=native_capability(),
            binary_files=native_capability(),
            atomic_rename=native_capability(),
            file_hash=native_capability(algorithm="sha256", adapter="client"),
            command_execution=native_capability(),
            streaming_output=native_capability(),
            command_stdin=unavailable_capability("OpenSandbox run has no stdin field"),
            pause_resume=native_capability(),
            bulk_upload=native_capability(),
        )

    async def list_files(self, ref: SandboxRef, path: str) -> list[FileEntry]:
        sandbox = await self._get_sandbox(ref)
        try:
            entries = await sandbox.files.list_directory(DirectoryListEntry(path=path, depth=1))
            return [self._file_entry(entry) for entry in entries]
        except Exception as error:
            raise self._normalize_error("file.list", error, ref=ref) from error

    async def read_file(self, ref: SandboxRef, path: str) -> bytes:
        sandbox = await self._get_sandbox(ref)
        try:
            return await sandbox.files.read_bytes(path)
        except Exception as error:
            raise self._normalize_error("file.read", error, ref=ref) from error

    async def write_file(self, ref: SandboxRef, request: WriteFileRequest) -> WriteFileResult:
        sandbox = await self._get_sandbox(ref)
        try:
            previous = None
            if request.expected_hash is not None:
                previous = await self.read_file(ref, request.path)
                actual_hash = self._hash(previous)
                if actual_hash != request.expected_hash:
                    raise ConcurrentModificationError(
                        "Expected file hash does not match",
                        provider_name=self.provider_name,
                        provider_key=self.provider_key,
                        sandbox_id=ref.sandbox_id,
                        sandbox_instance_id=ref.sandbox_instance_id,
                        operation="file.write",
                        details={
                            "expected_hash": request.expected_hash,
                            "actual_hash": actual_hash,
                        },
                    )
            await sandbox.files.write_file(request.path, request.content)
            content_hash = self._hash(request.content)
            return WriteFileResult(
                entry=FileEntry(
                    path=request.path,
                    kind=FileKind.FILE,
                    size=len(request.content),
                    content_hash=content_hash,
                ),
                previous_hash=self._hash(previous) if previous is not None else None,
                cas_strength="emulated" if request.expected_hash is not None else "native",
            )
        except ConcurrentModificationError:
            raise
        except Exception as error:
            raise self._normalize_error("file.write", error, ref=ref) from error

    async def delete_file(self, ref: SandboxRef, path: str) -> None:
        sandbox = await self._get_sandbox(ref)
        try:
            await sandbox.files.delete_files([path])
        except Exception as error:
            raise self._normalize_error("file.delete", error, ref=ref) from error

    async def execute(self, ref: SandboxRef, request: ExecRequest) -> ExecResult:
        return await self._execute(ref, request, on_output=None)

    async def execute_stream(
        self,
        ref: SandboxRef,
        request: ExecRequest,
        on_output: Callable[[CommandStream, bytes], Awaitable[None]],
    ) -> ExecResult:
        return await self._execute(ref, request, on_output=on_output)

    async def _execute(
        self,
        ref: SandboxRef,
        request: ExecRequest,
        *,
        on_output: Callable[[CommandStream, bytes], Awaitable[None]] | None,
    ) -> ExecResult:
        sandbox = await self._get_sandbox(ref)
        started_at = time.perf_counter_ns()
        execution_id: str | None = None
        stdout_buffer = BoundedHeadTailBuffer(
            max_bytes=self._command_result_config.max_stdout_bytes,
            tail_bytes=self._command_result_config.preserve_tail_bytes,
        )
        stderr_buffer = BoundedHeadTailBuffer(
            max_bytes=self._command_result_config.max_stderr_bytes,
            tail_bytes=self._command_result_config.preserve_tail_bytes,
        )

        async def capture_init(messages: Any) -> None:
            nonlocal execution_id
            normalized = cast(
                list[_ExecutionInitMessage],
                messages if isinstance(messages, list) else [messages],
            )
            if normalized:
                execution_id = str(normalized[-1].id)

        async def capture_stdout(messages: Any) -> None:
            normalized = cast(
                list[_ExecutionOutputMessage],
                messages if isinstance(messages, list) else [messages],
            )
            for message in normalized:
                data = str(message.text).encode("utf-8")
                stdout_buffer.append(data)
                if on_output is not None:
                    await on_output(CommandStream.STDOUT, data)

        async def capture_stderr(messages: Any) -> None:
            normalized = cast(
                list[_ExecutionOutputMessage],
                messages if isinstance(messages, list) else [messages],
            )
            for message in normalized:
                data = str(message.text).encode("utf-8")
                stderr_buffer.append(data)
                if on_output is not None:
                    await on_output(CommandStream.STDERR, data)

        opts = RunCommandOpts(
            working_directory=request.cwd,
            timeout=(
                timedelta(seconds=request.timeout_seconds)
                if request.timeout_seconds
                else None
            ),
            envs=request.env or None,
        )
        handlers = ExecutionHandlers(
            on_init=capture_init,
            on_stdout=capture_stdout,
            on_stderr=capture_stderr,
        )
        try:
            execution = await sandbox.commands.run(
                request.command,
                opts=opts,
                handlers=handlers,
            )
            resolved_execution_id = execution_id or execution.id or str(uuid7())
            execution_id = resolved_execution_id
            if stdout_buffer.total_bytes == 0:
                for message in execution.logs.stdout:
                    stdout_buffer.append(str(message.text).encode("utf-8"))
            if stderr_buffer.total_bytes == 0:
                for message in execution.logs.stderr:
                    stderr_buffer.append(str(message.text).encode("utf-8"))
            exit_code = execution.exit_code
            if execution.complete is None:
                state = CommandState.UNKNOWN
            elif exit_code == 0:
                state = CommandState.SUCCEEDED
            else:
                state = CommandState.FAILED
            return ExecResult(
                command_id=resolved_execution_id,
                stdout=stdout_buffer.value(),
                stderr=stderr_buffer.value(),
                exit_code=exit_code,
                state=state,
                duration_ms=self._duration_ms(started_at),
                stdout_truncated=stdout_buffer.truncated,
                stderr_truncated=stderr_buffer.truncated,
                stdout_total_bytes=stdout_buffer.total_bytes,
                stderr_total_bytes=stderr_buffer.total_bytes,
            )
        except asyncio.CancelledError:
            if execution_id is not None:
                try:
                    await sandbox.commands.interrupt(execution_id)
                except Exception:
                    pass
            raise
        except TimeoutError:
            return ExecResult(
                command_id=str(uuid7()),
                stdout=stdout_buffer.value(),
                stderr=stderr_buffer.value(),
                state=CommandState.TIMEOUT,
                duration_ms=self._duration_ms(started_at),
                stdout_truncated=stdout_buffer.truncated,
                stderr_truncated=stderr_buffer.truncated,
                stdout_total_bytes=stdout_buffer.total_bytes,
                stderr_total_bytes=stderr_buffer.total_bytes,
            )
        except Exception as error:
            raise self._normalize_error("command.execute", error, ref=ref) from error

    async def close(self) -> None:
        if self._manager is not None:
            await self._manager.close()
            self._manager = None
        for sandbox in {id(value): value for value in self._sandboxes.values()}.values():
            try:
                await sandbox.close()
            except Exception:
                continue
        self._sandboxes.clear()

    async def _get_sandbox(self, ref: SandboxRef) -> Any:
        self._validate_ref(ref)
        cache_key = self._cache_key(ref)
        sandbox = self._sandboxes.get(cache_key)
        if sandbox is not None:
            await self._validate_remote_identity(sandbox, ref)
            return sandbox
        try:
            sandbox = await self._sandbox_class.connect(
                ref.sandbox_id,
                connection_config=self._connection_config,
                skip_health_check=True,
            )
        except Exception as error:
            raise self._normalize_error("sandbox.connect", error, ref=ref) from error
        await self._validate_remote_identity(sandbox, ref)
        self._sandboxes[cache_key] = sandbox
        return sandbox

    async def _get_manager(self) -> Any:
        if self._manager is None:
            self._manager = await self._manager_class.create(self._connection_config)
        return self._manager

    def _validate_ref(self, ref: SandboxRef) -> None:
        if (
            ref.provider_name != self.provider_name
            or ref.provider_key != self.provider_key
            or ref.endpoint_fingerprint != self._endpoint_fingerprint()
        ):
            raise SandboxNotFoundError(
                "Sandbox reference does not belong to this OpenSandbox provider",
                provider_name=self.provider_name,
                provider_key=self.provider_key,
                sandbox_id=ref.sandbox_id,
                sandbox_instance_id=ref.sandbox_instance_id,
            )

    async def _validate_remote_identity(self, sandbox: Any, ref: SandboxRef) -> None:
        try:
            info = await sandbox.get_info()
        except Exception as error:
            raise self._normalize_error("sandbox.identity", error, ref=ref) from error
        remote_ref = self._ref_from_info(info)
        if remote_ref.sandbox_instance_id != ref.sandbox_instance_id:
            raise SandboxNotFoundError(
                "Sandbox instance identity no longer matches this reference",
                provider_name=self.provider_name,
                provider_key=self.provider_key,
                sandbox_id=ref.sandbox_id,
                sandbox_instance_id=ref.sandbox_instance_id,
                operation="sandbox.identity",
            )

    def _cache_key(self, ref: SandboxRef) -> tuple[str, str, str]:
        return (
            ref.endpoint_fingerprint or "",
            ref.sandbox_id,
            ref.sandbox_instance_id,
        )

    def _ref_from_info(self, info: Any) -> SandboxRef:
        metadata = dict(getattr(info, "metadata", None) or {})
        fallback = hashlib.sha256(
            f"{self.provider_key}:{info.id}".encode()
        ).hexdigest()[:32]
        instance_id = metadata.get("sandbox_instance_id") or f"legacy-{fallback}"
        metadata.setdefault("sandbox_instance_id", instance_id)
        return SandboxRef(
            provider_name=self.provider_name,
            provider_key=self.provider_key,
            sandbox_id=str(info.id),
            sandbox_instance_id=instance_id,
            endpoint_fingerprint=self._endpoint_fingerprint(),
            metadata=metadata,
        )

    def _info_from_sdk(self, info: Any, ref: SandboxRef | None = None) -> SandboxInfo:
        resolved_ref = ref or self._ref_from_info(info)
        image = getattr(getattr(info, "image", None), "image", None)
        return SandboxInfo(
            ref=resolved_ref,
            state=self._map_state(getattr(info, "status", None)),
            image=image,
            workdir="/workspace",
        )

    @staticmethod
    def _file_entry(entry: EntryInfo) -> FileEntry:
        kind = {
            "file": FileKind.FILE,
            "directory": FileKind.DIRECTORY,
            "dir": FileKind.DIRECTORY,
            "symlink": FileKind.SYMLINK,
        }.get(str(entry.entry_type).lower(), FileKind.OTHER)
        return FileEntry(path=entry.path, kind=kind, size=entry.size)

    @staticmethod
    def _map_state(status: Any) -> SandboxState:
        nested_state = getattr(status, "state", None)
        value = str(getattr(status, "value", nested_state or status)).lower()
        if value in {"running", "ready"}:
            return SandboxState.RUNNING
        if value == "paused":
            return SandboxState.PAUSED
        if value in {"pending", "creating"}:
            return SandboxState.CREATING
        if value in {"terminated", "stopped", "deleted"}:
            return SandboxState.STOPPED
        if value in {"failed", "error"}:
            return SandboxState.FAILED
        return SandboxState.UNKNOWN

    def _normalize_error(
        self,
        operation: str,
        error: Exception,
        *,
        ref: SandboxRef | None = None,
    ) -> SandboxBackendError:
        message = str(error)
        lower = message.lower()
        error_type = type(error).__name__.lower()
        status_code = getattr(error, "status_code", None)
        provider_error = getattr(error, "error", None)
        provider_error_code = getattr(provider_error, "code", None)
        request_id = getattr(error, "request_id", None)
        file_not_found = operation.startswith("file.") and (
            isinstance(error, KeyError)
            or provider_error_code == "FILE_NOT_FOUND"
            or "[file_not_found]" in lower
        )
        if file_not_found:
            normalized: SandboxBackendError = BackendFileNotFoundError(message)
        elif isinstance(error, KeyError) or (
            isinstance(status_code, int) and status_code == 404
        ):
            normalized = SandboxNotFoundError(message)
        elif isinstance(error, InvalidArgumentException):
            normalized = ProviderError(message, retryable=False)
        elif isinstance(error, SandboxReadyTimeoutException):
            normalized = ProviderError(message, retryable=True)
        elif isinstance(error, (SandboxUnhealthyException, SandboxInternalException)):
            normalized = ProviderError(message, retryable=True)
        elif isinstance(status_code, int):
            normalized = ProviderError(
                message,
                retryable=status_code == 408 or status_code == 429 or status_code >= 500,
            )
        elif provider_error_code in {"UNHEALTHY", "INTERNAL_UNKNOWN_ERROR"}:
            normalized = ProviderError(message, retryable=True)
        elif "paused" in lower or "invalid state" in lower:
            normalized = SandboxStateError(message)
        elif isinstance(error, SandboxException):
            normalized = ProviderError(message, retryable=False)
        elif "not found" in lower or "notfound" in error_type:
            normalized = SandboxNotFoundError(message)
        else:
            normalized = ProviderError(message, retryable=False)
        normalized.provider_name = self.provider_name
        normalized.provider_key = self.provider_key
        normalized.sandbox_id = ref.sandbox_id if ref else None
        normalized.sandbox_instance_id = ref.sandbox_instance_id if ref else None
        normalized.operation = operation
        normalized.provider_error_code = provider_error_code
        normalized.provider_request_id = request_id
        normalized.details = {
            "source_exception": error_type,
            "status_code": status_code,
            "provider_error_code": provider_error_code,
        }
        return normalized

    def _endpoint_fingerprint(self) -> str:
        domain = self._connection_config.domain or ""
        protocol = self._connection_config.protocol
        return hashlib.sha256(f"{protocol}://{domain}".encode()).hexdigest()[:16]

    @staticmethod
    def _hash(content: bytes | None) -> str | None:
        return hashlib.sha256(content).hexdigest() if content is not None else None

    @staticmethod
    def _duration_ms(started_at: int) -> int:
        return max(0, (time.perf_counter_ns() - started_at) // 1_000_000)
