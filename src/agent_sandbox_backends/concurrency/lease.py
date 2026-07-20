from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.domain.errors import (
    LockAcquisitionTimeoutError,
    SandboxLeaseExpiredError,
)
from agent_sandbox_backends.ports.lease_store import LeaseStore


@dataclass(slots=True)
class LeaseHandle:
    resource_key: str
    owner_id: str
    _lost: asyncio.Event = field(default_factory=asyncio.Event)
    renewed: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def valid(self) -> bool:
        return not self._lost.is_set()

    def ensure_valid(self) -> None:
        if not self.valid:
            raise SandboxLeaseExpiredError(
                f"Lease expired for {self.resource_key}",
                details={"resource_key": self.resource_key, "owner_id": self.owner_id},
            )

    def mark_lost(self) -> None:
        self._lost.set()


class LeaseManager:
    def __init__(
        self,
        store: LeaseStore,
        *,
        owner_id: str | None = None,
        ttl_ms: int = 30_000,
        poll_interval_ms: int = 100,
    ) -> None:
        if ttl_ms < 3:
            raise ValueError("ttl_ms must be at least 3")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        self.store = store
        self.owner_id = owner_id or str(uuid7())
        self.ttl_ms = ttl_ms
        self.poll_interval_ms = poll_interval_ms

    @asynccontextmanager
    async def hold(
        self,
        resource_key: str,
        *,
        timeout_seconds: float = 30,
    ) -> AsyncGenerator[LeaseHandle, None]:
        handle = LeaseHandle(resource_key=resource_key, owner_id=self.owner_id)
        await self._acquire(handle, timeout_seconds=timeout_seconds)
        renewal = asyncio.create_task(
            self._renew_loop(handle),
            name=f"sandbox-lease:{resource_key}",
        )
        try:
            yield handle
            handle.ensure_valid()
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            await self.store.lease_release(resource_key, self.owner_id)

    async def _acquire(self, handle: LeaseHandle, *, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        while True:
            result = await self.store.lease_acquire(
                handle.resource_key,
                handle.owner_id,
                self.ttl_ms,
            )
            if result.get("acquired") is True:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LockAcquisitionTimeoutError(
                    f"Timed out acquiring lease for {handle.resource_key}",
                    retryable=True,
                    details={"resource_key": handle.resource_key},
                )
            await asyncio.sleep(min(self.poll_interval_ms / 1000, remaining))

    async def _renew_loop(self, handle: LeaseHandle) -> None:
        interval = self.ttl_ms / 3000
        try:
            while True:
                await asyncio.sleep(interval)
                result = await self.store.lease_renew(
                    handle.resource_key,
                    handle.owner_id,
                    self.ttl_ms,
                )
                if result.get("renewed") is not True:
                    handle.mark_lost()
                    return
                handle.renewed.set()
        except asyncio.CancelledError:
            raise
        except Exception:
            handle.mark_lost()
