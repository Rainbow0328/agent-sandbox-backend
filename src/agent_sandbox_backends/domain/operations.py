from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel
from agent_sandbox_backends.domain.context import ActorContext
from agent_sandbox_backends.domain.identity import SandboxRef


class OperationStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class OperationEvent(DomainModel):
    event_id: str
    occurred_at: datetime
    operation_type: str
    status: OperationStatus
    actor: ActorContext
    sandbox_ref: SandboxRef | None = None
    request: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    error_code: str | None = None
    schema_version: int = 1

    @property
    def operation_id(self) -> str:
        return self.event_id
