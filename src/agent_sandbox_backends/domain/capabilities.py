from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel


class CapabilitySupport(StrEnum):
    NATIVE = "native"
    EMULATED = "emulated"
    UNAVAILABLE = "unavailable"


class Capability(DomainModel):
    supported: bool
    strength: CapabilitySupport
    limits: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None
    source: str = "provider"


class Capabilities(DomainModel):
    filesystem: Capability
    binary_files: Capability
    atomic_rename: Capability
    file_hash: Capability
    command_execution: Capability
    streaming_output: Capability
    command_stdin: Capability
    pause_resume: Capability
    bulk_upload: Capability

    def require(self, name: str) -> Capability:
        capability = getattr(self, name, None)
        if not isinstance(capability, Capability) or not capability.supported:
            from agent_sandbox_backends.domain.errors import UnsupportedCapabilityError

            raise UnsupportedCapabilityError(operation=name)
        return capability


def native_capability(**limits: Any) -> Capability:
    return Capability(
        supported=True,
        strength=CapabilitySupport.NATIVE,
        limits=limits,
    )


def unavailable_capability(reason: str) -> Capability:
    return Capability(
        supported=False,
        strength=CapabilitySupport.UNAVAILABLE,
        reason=reason,
    )
