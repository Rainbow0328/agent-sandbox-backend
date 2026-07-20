from enum import StrEnum


class BackendMode(StrEnum):
    CREATE = "create"
    CONNECT = "connect"


class CleanupPolicy(StrEnum):
    ON_CLOSE = "on_close"
    TTL = "ttl"
    NEVER = "never"
