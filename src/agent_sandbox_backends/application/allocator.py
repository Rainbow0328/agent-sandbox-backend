from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from agent_sandbox_backends.application.backend import SandboxBackend
from agent_sandbox_backends.config.concurrency import SandboxSharingMode
from agent_sandbox_backends.config.models import CleanupPolicy
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.ports.provider import SandboxProvider

BackendFactory = Callable[..., Awaitable[SandboxBackend]]


class SandboxAllocator:
    def __init__(
        self,
        *,
        provider: SandboxProvider,
        backend_factory: BackendFactory,
        mode: SandboxSharingMode = SandboxSharingMode.SHARED,
        cleanup: CleanupPolicy = CleanupPolicy.ON_CLOSE,
        owns_provider: bool = False,
        backend_options: Mapping[str, Any] | None = None,
    ) -> None:
        self.provider = provider
        self.mode = mode
        self.cleanup = cleanup
        self._owns_provider = owns_provider
        self._backend_factory = backend_factory
        self._backend_options = dict(backend_options or {})
        self._lock = asyncio.Lock()
        self._shared_ref: SandboxRef | None = None
        self._active: dict[int, SandboxBackend] = {}
        self._closed = False

    async def allocate(
        self,
        *,
        agent_id: str,
        parent_ref: SandboxRef | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> SandboxBackend:
        async with self._lock:
            if self._closed:
                raise RuntimeError("SandboxAllocator is closed")
            allocation_metadata = self._allocation_metadata(
                agent_id=agent_id,
                parent_ref=parent_ref,
                metadata=metadata,
            )
            if self.mode == SandboxSharingMode.SHARED and self._shared_ref is not None:
                backend = await self._backend_factory(
                    provider=self.provider,
                    mode="connect",
                    ref=self._shared_ref,
                    cleanup=CleanupPolicy.NEVER,
                    close_coordinator=self._release,
                    **self._backend_options,
                )
            else:
                backend = await self._backend_factory(
                    provider=self.provider,
                    mode="create",
                    metadata=allocation_metadata,
                    cleanup=CleanupPolicy.NEVER,
                    close_coordinator=self._release,
                    **self._backend_options,
                )
                if self.mode == SandboxSharingMode.SHARED:
                    self._shared_ref = backend.ref
            self._active[id(backend)] = backend
            return backend

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            active = tuple(self._active.values())
        for backend in active:
            await backend.close()
        if self._owns_provider:
            await self.provider.close()

    async def __aenter__(self) -> SandboxAllocator:
        if self._closed:
            raise RuntimeError("SandboxAllocator is closed")
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def _release(
        self,
        backend: SandboxBackend,
        delete_sandbox: Callable[[], Awaitable[None]],
    ) -> None:
        async with self._lock:
            tracked = self._active.pop(id(backend), None)
            if tracked is None:
                return
            if self.mode == SandboxSharingMode.ISOLATED:
                if self.cleanup == CleanupPolicy.ON_CLOSE:
                    try:
                        await delete_sandbox()
                    except Exception:
                        self._active[id(backend)] = backend
                        raise
                return
            if self._active:
                return
            if self.cleanup == CleanupPolicy.ON_CLOSE:
                try:
                    await delete_sandbox()
                except Exception:
                    self._active[id(backend)] = backend
                    raise
                self._shared_ref = None

    def _allocation_metadata(
        self,
        *,
        agent_id: str,
        parent_ref: SandboxRef | None,
        metadata: Mapping[str, str] | None,
    ) -> dict[str, str]:
        result = dict(metadata or {})
        result["agent_sandbox.agent_id"] = agent_id
        result["agent_sandbox.mode"] = self.mode.value
        if parent_ref is not None:
            result["agent_sandbox.parent_sandbox_id"] = parent_ref.sandbox_id
            result["agent_sandbox.parent_instance_id"] = parent_ref.sandbox_instance_id
        return result
