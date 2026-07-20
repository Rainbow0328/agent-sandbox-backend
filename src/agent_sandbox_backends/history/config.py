from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, model_validator

from agent_sandbox_backends.domain.base import DomainModel


class HistoryMode(StrEnum):
    NONE = "none"
    PROVIDER = "provider"
    DATABASE = "database"
    SANDBOX = "sandbox"


class HistoryConsistency(StrEnum):
    STRICT_START = "strict_start"
    BEST_EFFORT = "best_effort"


class OverflowPolicy(StrEnum):
    DELETE_OLDEST = "delete_oldest"
    STOP_RECORDING = "stop_recording"
    FAIL_OPERATION = "fail_operation"


class Compression(StrEnum):
    IDENTITY = "identity"
    ZLIB = "zlib"
    GZIP = "gzip"


class ConfigSync(StrEnum):
    INITIALIZE_OR_ADOPT = "initialize_or_adopt"
    REQUIRE_MATCH = "require_match"
    OVERWRITE = "overwrite"


class HistoryConfig(DomainModel):
    mode: HistoryMode = HistoryMode.SANDBOX
    consistency: HistoryConsistency = HistoryConsistency.STRICT_START
    ttl_days: int | None = Field(default=7, ge=1)
    max_database_bytes: int = Field(default=128 * 1024 * 1024, ge=16 * 1024 * 1024)
    max_operation_output_bytes: int = Field(default=16 * 1024 * 1024, ge=1)
    overflow_policy: OverflowPolicy = OverflowPolicy.DELETE_OLDEST
    capture_stdout: bool = True
    capture_stderr: bool = True
    output_chunk_bytes: int = Field(default=16 * 1024, ge=4 * 1024, le=256 * 1024)
    output_flush_bytes: int = Field(default=32 * 1024, ge=1, le=1024 * 1024)
    output_flush_interval_ms: int = Field(default=50, ge=0, le=1000)
    output_queue_max_bytes: int = Field(
        default=4 * 1024 * 1024,
        ge=4 * 1024,
        le=64 * 1024 * 1024,
    )
    output_queue_max_chunks: int = Field(default=256, ge=1, le=4096)
    output_write_timeout_seconds: float = Field(default=10, gt=0, le=300)
    cleanup_min_interval_seconds: int = Field(default=3600, ge=0, le=86_400)
    consumer_active_ttl_days: int = Field(default=30, ge=1, le=3650)
    compression: Compression = Compression.IDENTITY
    compression_min_bytes: int = Field(default=4 * 1024, ge=0)
    compression_level: int = Field(default=1, ge=1, le=9)
    sqlite_busy_timeout_ms: int = Field(default=5000, ge=100, le=60_000)
    sqlite_cache_bytes: int = Field(default=4 * 1024 * 1024, ge=1024 * 1024, le=64 * 1024 * 1024)
    helper_query_max_bytes: int = Field(default=4 * 1024 * 1024, ge=64 * 1024, le=32 * 1024 * 1024)
    helper_envelope_max_bytes: int = Field(default=1024 * 1024, ge=64 * 1024, le=16 * 1024 * 1024)
    cleanup_interval_operations: int = Field(default=100, ge=1, le=10_000)
    cleanup_on_connect: bool = True
    stale_started_after_seconds: int = Field(default=300, ge=30, le=86_400)
    sync_retry_attempts: int = Field(default=3, ge=0, le=10)
    sync_retry_base_delay_ms: int = Field(default=50, ge=0, le=5000)
    sync_retry_max_delay_ms: int = Field(default=500, ge=0, le=5000)
    config_sync: ConfigSync = ConfigSync.INITIALIZE_OR_ADOPT

    @model_validator(mode="after")
    def validate_relationships(self) -> HistoryConfig:
        if self.max_operation_output_bytes >= self.max_database_bytes:
            raise ValueError("max_operation_output_bytes must be less than max_database_bytes")
        if self.sync_retry_max_delay_ms < self.sync_retry_base_delay_ms:
            raise ValueError("sync_retry_max_delay_ms must be >= sync_retry_base_delay_ms")
        if self.output_chunk_bytes > self.output_queue_max_bytes:
            raise ValueError("output_chunk_bytes must be <= output_queue_max_bytes")
        if self.output_flush_bytes > self.output_queue_max_bytes:
            raise ValueError("output_flush_bytes must be <= output_queue_max_bytes")
        return self

    def persisted_values(self) -> dict[str, Any]:
        excluded = {
            "mode",
            "consistency",
            "sync_retry_attempts",
            "sync_retry_base_delay_ms",
            "sync_retry_max_delay_ms",
            "config_sync",
        }
        return self.model_dump(mode="json", exclude=excluded)
