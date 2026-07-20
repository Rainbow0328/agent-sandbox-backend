from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel


class SandboxSharingMode(StrEnum):
    SHARED = "shared"
    ISOLATED = "isolated"


class FileConflictMode(StrEnum):
    COMPARE_AND_SWAP = "compare_and_swap"
    PROCESS_LOCK = "process_lock"


class CommandIsolation(StrEnum):
    NONE = "none"
    ISOLATED_SANDBOX = "isolated_sandbox"


class ConcurrencyConfig(DomainModel):
    sandbox_mode: SandboxSharingMode = SandboxSharingMode.SHARED
    max_parallel_commands: int = Field(default=4, ge=1, le=256)
    max_parallel_file_reads: int = Field(default=32, ge=1, le=1024)
    max_parallel_file_writes: int = Field(default=8, ge=1, le=256)
    max_parallel_uploads: int = Field(default=1, ge=1, le=64)
    file_conflict: FileConflictMode = FileConflictMode.COMPARE_AND_SWAP
    lifecycle_lock_timeout: float = Field(default=30, gt=0, le=3600)
    command_queue_timeout: float = Field(default=60, gt=0, le=3600)
    file_queue_timeout: float = Field(default=60, gt=0, le=3600)
    upload_queue_timeout: float = Field(default=300, gt=0, le=7200)
    command_isolation: CommandIsolation = CommandIsolation.NONE
