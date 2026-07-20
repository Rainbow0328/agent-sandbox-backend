from __future__ import annotations

from pydantic import Field, model_validator

from agent_sandbox_backends.domain.base import DomainModel


class CommandResultConfig(DomainModel):
    max_stdout_bytes: int = Field(default=1024 * 1024, ge=0)
    max_stderr_bytes: int = Field(default=1024 * 1024, ge=0)
    preserve_tail_bytes: int = Field(default=256 * 1024, ge=0)

    @model_validator(mode="after")
    def validate_tail_capacity(self) -> CommandResultConfig:
        maximum = max(self.max_stdout_bytes, self.max_stderr_bytes)
        if self.preserve_tail_bytes > maximum:
            raise ValueError(
                "preserve_tail_bytes cannot exceed both stream byte limits"
            )
        return self
