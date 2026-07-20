from __future__ import annotations

from enum import StrEnum

from pydantic import Field, model_validator

from agent_sandbox_backends.domain.base import DomainModel


class CommandState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class CommandStream(StrEnum):
    STDOUT = "stdout"
    STDERR = "stderr"


class ExecRequest(DomainModel):
    command: str = Field(min_length=1)
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)
    stdin: bytes | None = None
    idempotency_key: str | None = None


class ExecResult(DomainModel):
    command_id: str
    stdout: bytes = b""
    stderr: bytes = b""
    exit_code: int | None = None
    state: CommandState
    duration_ms: int = Field(ge=0)
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    stdout_total_bytes: int = Field(default=0, ge=0)
    stderr_total_bytes: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_total_bytes(self) -> ExecResult:
        if self.stdout_total_bytes == 0 and self.stdout:
            object.__setattr__(self, "stdout_total_bytes", len(self.stdout))
        if self.stderr_total_bytes == 0 and self.stderr:
            object.__setattr__(self, "stderr_total_bytes", len(self.stderr))
        if self.stdout_total_bytes < len(self.stdout):
            raise ValueError("stdout_total_bytes cannot be smaller than stdout")
        if self.stderr_total_bytes < len(self.stderr):
            raise ValueError("stderr_total_bytes cannot be smaller than stderr")
        return self

    @property
    def stdout_text(self) -> str:
        return self.stdout.decode("utf-8", errors="replace")

    @property
    def stderr_text(self) -> str:
        return self.stderr.decode("utf-8", errors="replace")


class CommandOutputChunk(DomainModel):
    command_id: str
    stream: CommandStream
    chunk_index: int = Field(ge=0)
    data: bytes
    final: bool = False
