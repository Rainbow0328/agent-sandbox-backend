from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import PurePosixPath

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
    FileNotFoundError,
    SandboxNotFoundError,
    SandboxStateError,
)
from agent_sandbox_backends.domain.files import (
    FileEntry,
    FileKind,
    WriteFileRequest,
    WriteFileResult,
)
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.sandbox import (
    CreateSandboxRequest,
    SandboxInfo,
    SandboxState,
)


@dataclass(slots=True)
class _SandboxRecord:
    info: SandboxInfo
    files: dict[str, bytes] = field(default_factory=lambda: dict[str, bytes]())
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(frozen=True, slots=True)
class _CommandBehavior:
    stdout: bytes
    stderr: bytes
    exit_code: int
    delay_seconds: float = 0


class MockSandboxProvider:
    provider_name = "mock"

    def __init__(
        self,
        *,
        provider_key: str = "mock-default",
        command_result_config: CommandResultConfig | None = None,
    ) -> None:
        self.provider_key = provider_key
        self._command_result_config = command_result_config or CommandResultConfig()
        self._sandboxes: dict[str, _SandboxRecord] = {}
        self._create_keys: dict[str, SandboxRef] = {}
        self._command_behaviors: dict[str, _CommandBehavior] = {}
        self._lock = asyncio.Lock()

    def set_command_result(
        self,
        command: str,
        *,
        stdout: bytes = b"",
        stderr: bytes = b"",
        exit_code: int = 0,
        delay_seconds: float = 0,
    ) -> None:
        self._command_behaviors[command] = _CommandBehavior(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            delay_seconds=delay_seconds,
        )

    async def create(self, request: CreateSandboxRequest) -> SandboxRef:
        async with self._lock:
            if request.idempotency_key is not None:
                existing = self._create_keys.get(request.idempotency_key)
                if existing is not None:
                    return existing

            sandbox_id = f"sandbox-{uuid7()}"
            ref = SandboxRef(
                provider_name=self.provider_name,
                provider_key=self.provider_key,
                sandbox_id=sandbox_id,
                sandbox_instance_id=str(uuid7()),
                metadata=dict(request.metadata),
            )
            info = SandboxInfo(
                ref=ref,
                state=SandboxState.RUNNING,
                image=request.image,
                workdir=self._normalize_path(request.workdir, "/"),
            )
            self._sandboxes[sandbox_id] = _SandboxRecord(info=info)
            if request.idempotency_key is not None:
                self._create_keys[request.idempotency_key] = ref
            return ref

    async def get(self, ref: SandboxRef) -> SandboxInfo:
        return self._record(ref).info

    async def list(self) -> list[SandboxInfo]:
        return [record.info for record in self._sandboxes.values()]

    async def pause(self, ref: SandboxRef) -> SandboxInfo:
        record = self._record(ref)
        async with record.lock:
            self._require_state(record, SandboxState.RUNNING)
            record.info = record.info.model_copy(update={"state": SandboxState.PAUSED})
            return record.info

    async def resume(self, ref: SandboxRef) -> SandboxInfo:
        record = self._record(ref)
        async with record.lock:
            self._require_state(record, SandboxState.PAUSED)
            record.info = record.info.model_copy(update={"state": SandboxState.RUNNING})
            return record.info

    async def delete(self, ref: SandboxRef) -> None:
        async with self._lock:
            record = self._sandboxes.get(ref.sandbox_id)
            if record is None:
                return
            self._validate_ref(ref, record)
            del self._sandboxes[ref.sandbox_id]

    async def capabilities(self, ref: SandboxRef | None = None) -> Capabilities:
        if ref is not None:
            self._record(ref)
        return Capabilities(
            filesystem=native_capability(),
            binary_files=native_capability(),
            atomic_rename=native_capability(),
            file_hash=native_capability(algorithm="sha256"),
            command_execution=native_capability(),
            streaming_output=unavailable_capability("mock commands return completed results"),
            command_stdin=unavailable_capability("mock stdin is not implemented"),
            pause_resume=native_capability(),
            bulk_upload=unavailable_capability("mock bulk upload is not implemented"),
        )

    async def close(self) -> None:
        return None

    async def list_files(self, ref: SandboxRef, path: str) -> list[FileEntry]:
        record = self._running_record(ref)
        root = self._normalize_path(path, record.info.workdir)
        prefix = root.rstrip("/") + "/"
        entries: dict[str, FileEntry] = {}

        async with record.lock:
            for file_path, content in record.files.items():
                if not file_path.startswith(prefix):
                    continue
                remainder = file_path[len(prefix) :]
                name, separator, _ = remainder.partition("/")
                entry_path = prefix + name
                if separator:
                    entries[entry_path] = FileEntry(
                        path=entry_path,
                        kind=FileKind.DIRECTORY,
                        size=0,
                    )
                else:
                    entries[entry_path] = self._file_entry(entry_path, content)
        return sorted(entries.values(), key=lambda entry: entry.path)

    async def read_file(self, ref: SandboxRef, path: str) -> bytes:
        record = self._running_record(ref)
        normalized = self._normalize_path(path, record.info.workdir)
        async with record.lock:
            try:
                return record.files[normalized]
            except KeyError as error:
                raise FileNotFoundError(
                    f"File does not exist: {normalized}",
                    provider_name=self.provider_name,
                    provider_key=self.provider_key,
                    sandbox_id=ref.sandbox_id,
                    sandbox_instance_id=ref.sandbox_instance_id,
                    operation="file.read",
                ) from error

    async def write_file(
        self,
        ref: SandboxRef,
        request: WriteFileRequest,
    ) -> WriteFileResult:
        record = self._running_record(ref)
        normalized = self._normalize_path(request.path, record.info.workdir)
        async with record.lock:
            previous = record.files.get(normalized)
            previous_hash = self._hash(previous) if previous is not None else None
            if request.expected_hash is not None and request.expected_hash != previous_hash:
                raise ConcurrentModificationError(
                    f"Expected hash does not match current file: {normalized}",
                    provider_name=self.provider_name,
                    provider_key=self.provider_key,
                    sandbox_id=ref.sandbox_id,
                    sandbox_instance_id=ref.sandbox_instance_id,
                    operation="file.write",
                    details={
                        "expected_hash": request.expected_hash,
                        "actual_hash": previous_hash,
                    },
                )
            record.files[normalized] = request.content
            return WriteFileResult(
                entry=self._file_entry(normalized, request.content),
                previous_hash=previous_hash,
            )

    async def delete_file(self, ref: SandboxRef, path: str) -> None:
        record = self._running_record(ref)
        normalized = self._normalize_path(path, record.info.workdir)
        async with record.lock:
            if normalized not in record.files:
                raise FileNotFoundError(
                    f"File does not exist: {normalized}",
                    provider_name=self.provider_name,
                    provider_key=self.provider_key,
                    sandbox_id=ref.sandbox_id,
                    sandbox_instance_id=ref.sandbox_instance_id,
                    operation="file.delete",
                )
            del record.files[normalized]

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
        record = self._running_record(ref)
        started_at = time.perf_counter_ns()
        behavior = self._command_behaviors.get(request.command)
        if behavior is None:
            behavior = self._default_command_behavior(record, request)
        if behavior.delay_seconds:
            if request.timeout_seconds is None:
                await asyncio.sleep(behavior.delay_seconds)
            else:
                try:
                    async with asyncio.timeout(request.timeout_seconds):
                        await asyncio.sleep(behavior.delay_seconds)
                except TimeoutError:
                    return ExecResult(
                        command_id=str(uuid7()),
                        state=CommandState.TIMEOUT,
                        duration_ms=self._duration_ms(started_at),
                    )

        state = CommandState.SUCCEEDED if behavior.exit_code == 0 else CommandState.FAILED
        if on_output is not None and behavior.stdout:
            await on_output(CommandStream.STDOUT, behavior.stdout)
        if on_output is not None and behavior.stderr:
            await on_output(CommandStream.STDERR, behavior.stderr)
        stdout = self._bounded_result(
            behavior.stdout,
            self._command_result_config.max_stdout_bytes,
        )
        stderr = self._bounded_result(
            behavior.stderr,
            self._command_result_config.max_stderr_bytes,
        )
        return ExecResult(
            command_id=str(uuid7()),
            stdout=stdout.value(),
            stderr=stderr.value(),
            exit_code=behavior.exit_code,
            state=state,
            duration_ms=self._duration_ms(started_at),
            stdout_truncated=stdout.truncated,
            stderr_truncated=stderr.truncated,
            stdout_total_bytes=stdout.total_bytes,
            stderr_total_bytes=stderr.total_bytes,
        )

    def _bounded_result(self, data: bytes, max_bytes: int) -> BoundedHeadTailBuffer:
        buffer = BoundedHeadTailBuffer(
            max_bytes=max_bytes,
            tail_bytes=self._command_result_config.preserve_tail_bytes,
        )
        buffer.append(data)
        return buffer

    def _default_command_behavior(
        self,
        record: _SandboxRecord,
        request: ExecRequest,
    ) -> _CommandBehavior:
        if request.command == "pwd":
            cwd = request.cwd or record.info.workdir
            return _CommandBehavior(stdout=f"{cwd}\n".encode(), stderr=b"", exit_code=0)
        if request.command == "false":
            return _CommandBehavior(stdout=b"", stderr=b"", exit_code=1)
        if request.command.startswith("echo "):
            return _CommandBehavior(
                stdout=f"{request.command[5:]}\n".encode(),
                stderr=b"",
                exit_code=0,
            )
        return _CommandBehavior(stdout=b"", stderr=b"", exit_code=0)

    def _record(self, ref: SandboxRef) -> _SandboxRecord:
        record = self._sandboxes.get(ref.sandbox_id)
        if record is None:
            raise SandboxNotFoundError(
                f"Sandbox does not exist: {ref.sandbox_id}",
                provider_name=self.provider_name,
                provider_key=self.provider_key,
                sandbox_id=ref.sandbox_id,
                sandbox_instance_id=ref.sandbox_instance_id,
            )
        self._validate_ref(ref, record)
        return record

    def _running_record(self, ref: SandboxRef) -> _SandboxRecord:
        record = self._record(ref)
        self._require_state(record, SandboxState.RUNNING)
        return record

    def _validate_ref(self, ref: SandboxRef, record: _SandboxRecord) -> None:
        if (
            ref.provider_name != self.provider_name
            or ref.provider_key != self.provider_key
            or ref.sandbox_instance_id != record.info.ref.sandbox_instance_id
        ):
            raise SandboxNotFoundError(
                "Sandbox reference does not match this provider instance",
                provider_name=self.provider_name,
                provider_key=self.provider_key,
                sandbox_id=ref.sandbox_id,
                sandbox_instance_id=ref.sandbox_instance_id,
            )

    def _require_state(self, record: _SandboxRecord, expected: SandboxState) -> None:
        if record.info.state != expected:
            raise SandboxStateError(
                f"Sandbox state must be {expected}, got {record.info.state}",
                provider_name=self.provider_name,
                provider_key=self.provider_key,
                sandbox_id=record.info.ref.sandbox_id,
                sandbox_instance_id=record.info.ref.sandbox_instance_id,
            )

    @staticmethod
    def _normalize_path(path: str, workdir: str) -> str:
        candidate = PurePosixPath(path)
        if not candidate.is_absolute():
            candidate = PurePosixPath(workdir) / candidate
        parts: list[str] = []
        for part in candidate.parts:
            if part in {"", "/", "."}:
                continue
            if part == "..":
                raise ValueError("Parent path traversal is not allowed")
            parts.append(part)
        return "/" + "/".join(parts)

    @classmethod
    def _file_entry(cls, path: str, content: bytes) -> FileEntry:
        return FileEntry(
            path=path,
            kind=FileKind.FILE,
            size=len(content),
            content_hash=cls._hash(content),
        )

    @staticmethod
    def _hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _duration_ms(started_at: int) -> int:
        return max(0, (time.perf_counter_ns() - started_at) // 1_000_000)
