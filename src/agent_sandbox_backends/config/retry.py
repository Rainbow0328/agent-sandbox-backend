from __future__ import annotations

from pydantic import Field, model_validator

from agent_sandbox_backends.domain.base import DomainModel


class RetryConfig(DomainModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    base_delay_ms: int = Field(default=100, ge=0, le=10_000)
    max_delay_ms: int = Field(default=2_000, ge=0, le=60_000)
    jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    total_deadline_seconds: float | None = Field(default=30, gt=0, le=3600)

    @model_validator(mode="after")
    def validate_delays(self) -> RetryConfig:
        if self.max_delay_ms < self.base_delay_ms:
            raise ValueError("max_delay_ms must be >= base_delay_ms")
        return self
