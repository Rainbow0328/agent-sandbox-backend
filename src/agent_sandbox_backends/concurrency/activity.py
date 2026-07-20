from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from agent_sandbox_backends.domain.errors import (
    LockAcquisitionTimeoutError,
    SandboxDeletingError,
)


class OperationActivityGate:
    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._active = 0
        self._closing = False

    @asynccontextmanager
    async def operation(self) -> AsyncGenerator[None, None]:
        async with self._condition:
            if self._closing:
                raise SandboxDeletingError("Sandbox close or deletion is in progress")
            self._active += 1
        try:
            yield
        finally:
            async with self._condition:
                self._active -= 1
                if self._active == 0:
                    self._condition.notify_all()

    async def begin_close(self, *, timeout_seconds: float) -> None:
        async with self._condition:
            self._closing = True
            try:
                async with asyncio.timeout(timeout_seconds):
                    await self._condition.wait_for(lambda: self._active == 0)
            except TimeoutError as error:
                self._closing = False
                self._condition.notify_all()
                raise LockAcquisitionTimeoutError(
                    "Timed out waiting for active sandbox operations to drain",
                    retryable=True,
                    details={"timeout_seconds": timeout_seconds},
                ) from error

    async def reopen(self) -> None:
        async with self._condition:
            self._closing = False
            self._condition.notify_all()
