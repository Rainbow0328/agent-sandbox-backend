from __future__ import annotations

import asyncio

from agent_sandbox_backends.domain.commands import CommandOutputChunk, CommandStream
from agent_sandbox_backends.domain.errors import HistoryDatabaseError, HistoryTransportError
from agent_sandbox_backends.history.config import HistoryConfig
from agent_sandbox_backends.ports.history_store import HistoryStore


class CommandHistoryOutputWriter:
    def __init__(
        self,
        history_store: HistoryStore,
        *,
        event_id: str,
        config: HistoryConfig,
    ) -> None:
        self.history_store = history_store
        self.event_id = event_id
        self.config = config
        self._buffers = {
            CommandStream.STDOUT: bytearray(),
            CommandStream.STDERR: bytearray(),
        }
        self._indexes = {CommandStream.STDOUT: 0, CommandStream.STDERR: 0}
        self._captured = 0
        self._lock = asyncio.Lock()
        self._timer: asyncio.Task[None] | None = None
        self._closed = False
        self._failed = False
        self._failure_reason: str | None = None
        self._truncated = False

    async def write(self, stream: CommandStream, data: bytes) -> None:
        if not data or self._closed or not self._capture_enabled(stream):
            return
        offset = 0
        async with self._lock:
            while offset < len(data):
                if self._failed:
                    return
                remaining = self.config.max_operation_output_bytes - self._captured
                if remaining <= 0:
                    self._truncated = True
                    return
                if self._queue_full():
                    await self._flush_locked()
                    if self._failed:
                        return
                available = self.config.output_queue_max_bytes - self._buffered_bytes()
                captured_bytes = min(len(data) - offset, remaining, available)
                if captured_bytes <= 0:
                    await self._flush_locked()
                    continue
                self._buffers[stream].extend(data[offset : offset + captured_bytes])
                self._captured += captured_bytes
                offset += captured_bytes
                self._ensure_timer()
                if (
                    self._buffered_bytes() >= self.config.output_flush_bytes
                    or self._queue_full()
                ):
                    await self._flush_locked()
            if offset < len(data):
                self._truncated = True

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            await self._flush_locked()
            timer = self._timer
            self._timer = None
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()
            try:
                await timer
            except asyncio.CancelledError:
                pass

    def _ensure_timer(self) -> None:
        if self.config.output_flush_interval_ms == 0 or self._timer is not None:
            return
        self._timer = asyncio.create_task(
            self._flush_periodically(),
            name=f"history-output-flush:{self.event_id}",
        )

    async def _flush_periodically(self) -> None:
        interval = self.config.output_flush_interval_ms / 1000
        try:
            while True:
                await asyncio.sleep(interval)
                async with self._lock:
                    if self._closed:
                        return
                    await self._flush_locked()
        except asyncio.CancelledError:
            raise

    async def _flush_locked(self) -> None:
        chunks: list[CommandOutputChunk] = []
        for stream, buffer in self._buffers.items():
            while buffer:
                data = bytes(buffer[: self.config.output_chunk_bytes])
                del buffer[: len(data)]
                index = self._indexes[stream]
                self._indexes[stream] += 1
                chunks.append(
                    CommandOutputChunk(
                        command_id=self.event_id,
                        stream=stream,
                        chunk_index=index,
                        data=data,
                        final=False,
                    )
                )
        if not chunks:
            return
        try:
            async with asyncio.timeout(self.config.output_write_timeout_seconds):
                await self.history_store.append_output(self.event_id, tuple(chunks))
        except (HistoryDatabaseError, HistoryTransportError, TimeoutError) as error:
            self._failed = True
            self._failure_reason = f"{type(error).__name__}: {error}"
            return

    def _capture_enabled(self, stream: CommandStream) -> bool:
        if stream == CommandStream.STDOUT:
            return self.config.capture_stdout
        return self.config.capture_stderr

    def _buffered_bytes(self) -> int:
        return sum(len(buffer) for buffer in self._buffers.values())

    def _buffered_chunks(self) -> int:
        chunk_bytes = self.config.output_chunk_bytes
        return sum(
            (len(buffer) + chunk_bytes - 1) // chunk_bytes
            for buffer in self._buffers.values()
        )

    def _queue_full(self) -> bool:
        return (
            self._buffered_bytes() >= self.config.output_queue_max_bytes
            or self._buffered_chunks() >= self.config.output_queue_max_chunks
        )

    @property
    def use_terminal_fallback(self) -> bool:
        return self._failed or self._captured == 0

    @property
    def output_complete(self) -> bool:
        return not self._failed and not self._truncated

    @property
    def storage_state(self) -> str:
        if self._failed:
            return "partial"
        if self._truncated:
            return "truncated_by_policy"
        return "complete"

    @property
    def failure_reason(self) -> str | None:
        return self._failure_reason
