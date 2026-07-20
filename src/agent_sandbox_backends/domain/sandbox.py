from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel
from agent_sandbox_backends.domain.identity import SandboxRef

SANDBOX_NAME_METADATA_KEY = "agent_sandbox.name"


class SandboxState(StrEnum):
    CREATING = "creating"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    UNKNOWN = "unknown"
    DELETED = "deleted"


class CreateSandboxRequest(DomainModel):
    image: str = Field(default="python:3.12", min_length=1)
    workdir: str = Field(default="/workspace", min_length=1)
    env: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str | None = None
    sandbox_ttl_seconds: float | None = Field(default=None, gt=0)


class SandboxInfo(DomainModel):
    ref: SandboxRef
    state: SandboxState
    image: str | None = None
    workdir: str
