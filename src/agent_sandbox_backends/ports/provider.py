from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from agent_sandbox_backends.domain.capabilities import Capabilities
from agent_sandbox_backends.domain.commands import CommandStream, ExecRequest, ExecResult
from agent_sandbox_backends.domain.files import FileEntry, WriteFileRequest, WriteFileResult
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.sandbox import CreateSandboxRequest, SandboxInfo


class SandboxLifecyclePort(Protocol):
    provider_name: str
    provider_key: str

    async def create(self, request: CreateSandboxRequest) -> SandboxRef: ...

    async def get(self, ref: SandboxRef) -> SandboxInfo: ...

    async def list(self) -> list[SandboxInfo]: ...

    async def pause(self, ref: SandboxRef) -> SandboxInfo: ...

    async def resume(self, ref: SandboxRef) -> SandboxInfo: ...

    async def delete(self, ref: SandboxRef) -> None: ...

    async def capabilities(self, ref: SandboxRef | None = None) -> Capabilities: ...

    async def close(self) -> None: ...


class SandboxFilesystemPort(Protocol):
    async def list_files(self, ref: SandboxRef, path: str) -> list[FileEntry]: ...

    async def read_file(self, ref: SandboxRef, path: str) -> bytes: ...

    async def write_file(
        self,
        ref: SandboxRef,
        request: WriteFileRequest,
    ) -> WriteFileResult: ...

    async def delete_file(self, ref: SandboxRef, path: str) -> None: ...


class SandboxCommandPort(Protocol):
    async def execute(self, ref: SandboxRef, request: ExecRequest) -> ExecResult: ...

    async def execute_stream(
        self,
        ref: SandboxRef,
        request: ExecRequest,
        on_output: Callable[[CommandStream, bytes], Awaitable[None]],
    ) -> ExecResult: ...


class SandboxProvider(SandboxLifecyclePort, SandboxFilesystemPort, SandboxCommandPort, Protocol):
    pass
