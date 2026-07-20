from __future__ import annotations

from typing import Protocol

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.operations import OperationEvent


class HistoryStore(Protocol):
    async def append(self, event: OperationEvent) -> None: ...

    async def append_output(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> None: ...

    async def close(self) -> None: ...
