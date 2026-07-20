from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from agent_sandbox_backends.domain.errors import SandboxBackendError


class QueueLimiter:
    def __init__(
        self,
        limit: int,
        *,
        timeout_seconds: float,
        error_type: type[SandboxBackendError],
        resource_name: str,
    ) -> None:
        self._semaphore = asyncio.Semaphore(limit)
        self.timeout_seconds = timeout_seconds
        self.error_type = error_type
        self.resource_name = resource_name

    @asynccontextmanager
    async def slot(self) -> AsyncGenerator[None, None]:
        acquired = False
        try:
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    await self._semaphore.acquire()
                    acquired = True
            except TimeoutError as error:
                raise self.error_type(
                    f"Timed out waiting for {self.resource_name}",
                    retryable=True,
                    details={"timeout_seconds": self.timeout_seconds},
                ) from error
            yield
        finally:
            if acquired:
                self._semaphore.release()
