from __future__ import annotations

from collections import deque


class BoundedHeadTailBuffer:
    def __init__(self, *, max_bytes: int, tail_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._tail_capacity = min(max_bytes, tail_bytes)
        self._head_capacity = max_bytes - self._tail_capacity
        self._head = bytearray()
        self._tail: deque[bytes] = deque()
        self._tail_size = 0
        self.total_bytes = 0

    @property
    def truncated(self) -> bool:
        return self.total_bytes > self._max_bytes

    def append(self, data: bytes) -> None:
        if not data:
            return
        self.total_bytes += len(data)
        head_missing = self._head_capacity - len(self._head)
        if head_missing > 0:
            self._head.extend(data[:head_missing])
            data = data[head_missing:]
        if self._tail_capacity == 0 or not data:
            return
        self._tail.append(data)
        self._tail_size += len(data)
        self._trim_tail()

    def value(self) -> bytes:
        if not self._tail:
            return bytes(self._head)
        return bytes(self._head) + b"".join(self._tail)

    def _trim_tail(self) -> None:
        overflow = self._tail_size - self._tail_capacity
        while overflow > 0 and self._tail:
            first = self._tail[0]
            if len(first) <= overflow:
                self._tail.popleft()
                self._tail_size -= len(first)
                overflow -= len(first)
                continue
            self._tail[0] = first[overflow:]
            self._tail_size -= overflow
            overflow = 0
