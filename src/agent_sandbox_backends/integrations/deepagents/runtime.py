from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from concurrent.futures import Future
from typing import Any, TypeVar

ResultT = TypeVar("ResultT")


class AsyncRuntimeBridge:
    """Run async backend calls from synchronous Deep Agents methods."""

    def __init__(self, *, thread_name: str = "agent-sandbox-backend") -> None:
        self._thread_name = thread_name
        self._ready = threading.Event()
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop: asyncio.Event | None = None
        self._thread = threading.Thread(target=self._run, name=thread_name, daemon=True)
        self._thread.start()
        self._ready.wait()

    def run(self, call: Coroutine[Any, Any, ResultT]) -> ResultT:
        if self._closed:
            call.close()
            raise RuntimeError("AsyncRuntimeBridge is closed")
        loop = self._require_loop()
        future: Future[ResultT] = asyncio.run_coroutine_threadsafe(call, loop)
        return future.result()

    def close(self, *, timeout_seconds: float = 5) -> None:
        if self._closed:
            return
        self._closed = True
        loop = self._require_loop()
        stop = self._stop
        if stop is not None:
            loop.call_soon_threadsafe(stop.set)
        self._thread.join(timeout_seconds)
        if self._thread.is_alive():
            raise TimeoutError(
                f"Runtime thread {self._thread_name!r} did not stop within {timeout_seconds}s"
            )

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._stop = asyncio.Event()
        self._ready.set()
        try:
            loop.run_until_complete(self._stop.wait())
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        finally:
            loop.close()

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError("AsyncRuntimeBridge failed to initialize")
        return self._loop
