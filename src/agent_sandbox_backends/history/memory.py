from __future__ import annotations

import asyncio

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.operations import OperationEvent


class MemoryHistoryStore:
    def __init__(self) -> None:
        self._events: list[OperationEvent] = []
        self._output_chunks: dict[str, list[CommandOutputChunk]] = {}
        self._lock = asyncio.Lock()

    async def append(self, event: OperationEvent) -> None:
        async with self._lock:
            self._events.append(event)

    async def events(self) -> tuple[OperationEvent, ...]:
        async with self._lock:
            return tuple(self._events)

    async def close(self) -> None:
        return None

    async def append_output(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> None:
        async with self._lock:
            self._output_chunks.setdefault(event_id, []).extend(chunks)

    async def output_chunks(self, event_id: str) -> tuple[CommandOutputChunk, ...]:
        async with self._lock:
            return tuple(self._output_chunks.get(event_id, ()))
