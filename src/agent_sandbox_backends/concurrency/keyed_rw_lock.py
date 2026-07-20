from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from agent_sandbox_backends.domain.errors import LockAcquisitionTimeoutError


@dataclass(slots=True)
class _LockState:
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    readers: int = 0
    writer: bool = False
    waiting_writers: int = 0
    references: int = 0


class KeyedRWLock:
    """Writer-preferring keyed read/write locks with idle-state cleanup."""

    def __init__(self) -> None:
        self._states: dict[str, _LockState] = {}
        self._states_lock = asyncio.Lock()

    @asynccontextmanager
    async def read(
        self,
        key: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AsyncGenerator[None, None]:
        state = await self._reference(key)
        acquired = False
        try:
            async with state.condition:
                await self._wait_for(
                    state.condition,
                    lambda: not state.writer and state.waiting_writers == 0,
                    key=key,
                    timeout_seconds=timeout_seconds,
                )
                state.readers += 1
                acquired = True
            yield
        finally:
            if acquired:
                async with state.condition:
                    state.readers -= 1
                    state.condition.notify_all()
            await self._dereference(key, state)

    @asynccontextmanager
    async def write(
        self,
        key: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AsyncGenerator[None, None]:
        state = await self._reference(key)
        acquired = False
        waiting = False
        try:
            async with state.condition:
                state.waiting_writers += 1
                waiting = True
                try:
                    await self._wait_for(
                        state.condition,
                        lambda: not state.writer and state.readers == 0,
                        key=key,
                        timeout_seconds=timeout_seconds,
                    )
                finally:
                    state.waiting_writers -= 1
                    waiting = False
                state.writer = True
                acquired = True
            yield
        finally:
            if waiting:
                async with state.condition:
                    state.waiting_writers -= 1
                    state.condition.notify_all()
            if acquired:
                async with state.condition:
                    state.writer = False
                    state.condition.notify_all()
            await self._dereference(key, state)

    async def size(self) -> int:
        async with self._states_lock:
            return len(self._states)

    async def _reference(self, key: str) -> _LockState:
        async with self._states_lock:
            state = self._states.setdefault(key, _LockState())
            state.references += 1
            return state

    async def _dereference(self, key: str, state: _LockState) -> None:
        async with self._states_lock:
            state.references -= 1
            if (
                state.references == 0
                and state.readers == 0
                and not state.writer
                and state.waiting_writers == 0
            ):
                self._states.pop(key, None)

    @staticmethod
    async def _wait_for(
        condition: asyncio.Condition,
        predicate: Callable[[], bool],
        *,
        key: str,
        timeout_seconds: float | None,
    ) -> None:
        async def wait() -> None:
            await condition.wait_for(predicate)

        try:
            if timeout_seconds is None:
                await wait()
            else:
                async with asyncio.timeout(timeout_seconds):
                    await wait()
        except TimeoutError as error:
            raise LockAcquisitionTimeoutError(
                f"Timed out acquiring lock for {key}",
                details={"resource_key": key, "timeout_seconds": timeout_seconds},
                retryable=True,
            ) from error
