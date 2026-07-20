from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.operations import OperationEvent


class NoneHistoryStore:
    async def append(self, event: OperationEvent) -> None:
        del event

    async def close(self) -> None:
        return None

    async def append_output(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> None:
        del event_id, chunks
