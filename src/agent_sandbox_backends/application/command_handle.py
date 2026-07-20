from __future__ import annotations

import asyncio

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.domain.commands import (
    CommandOutputChunk,
    CommandStream,
    ExecResult,
)


class CommandHandle:
    """Independent application-level handle for a running command."""

    def __init__(self, task: asyncio.Task[ExecResult]) -> None:
        self.handle_id = str(uuid7())
        self._task = task

    @property
    def done(self) -> bool:
        return self._task.done()

    async def result(self) -> ExecResult:
        return await self._task

    async def cancel(self) -> bool:
        if self._task.done():
            return False
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            return True
        return False

    async def output_chunks(
        self,
        *,
        chunk_bytes: int = 16 * 1024,
    ) -> tuple[CommandOutputChunk, ...]:
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        result = await self.result()
        chunks: list[CommandOutputChunk] = []
        for stream, content in (
            (CommandStream.STDOUT, result.stdout),
            (CommandStream.STDERR, result.stderr),
        ):
            parts = [
                content[offset : offset + chunk_bytes]
                for offset in range(0, len(content), chunk_bytes)
            ]
            for index, data in enumerate(parts):
                chunks.append(
                    CommandOutputChunk(
                        command_id=result.command_id,
                        stream=stream,
                        chunk_index=index,
                        data=data,
                        final=index + 1 == len(parts),
                    )
                )
        return tuple(chunks)
