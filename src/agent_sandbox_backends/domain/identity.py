from __future__ import annotations

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel


class SandboxRef(DomainModel):
    provider_name: str = Field(min_length=1)
    provider_key: str = Field(min_length=1)
    sandbox_id: str = Field(min_length=1)
    sandbox_instance_id: str = Field(min_length=1)
    endpoint_fingerprint: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
