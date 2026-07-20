from __future__ import annotations

import secrets
import threading
import time
import uuid

_lock = threading.Lock()
_last_millisecond = -1
_sequence = 0
_SEQUENCE_MASK = (1 << 74) - 1


def uuid7() -> uuid.UUID:
    """Return a process-monotonic UUIDv7 compatible identifier."""
    global _last_millisecond, _sequence

    millisecond = time.time_ns() // 1_000_000
    with _lock:
        if millisecond > _last_millisecond:
            _last_millisecond = millisecond
            _sequence = secrets.randbits(74)
        else:
            millisecond = _last_millisecond
            _sequence = (_sequence + 1) & _SEQUENCE_MASK
            if _sequence == 0:
                _last_millisecond += 1
                millisecond = _last_millisecond

        random_a = (_sequence >> 62) & 0xFFF
        random_b = _sequence & ((1 << 62) - 1)

    value = (millisecond & ((1 << 48) - 1)) << 80
    value |= 0x7 << 76
    value |= random_a << 64
    value |= 0b10 << 62
    value |= random_b
    return uuid.UUID(int=value)
