from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel


class UploadConflict(StrEnum):
    ERROR = "error"
    OVERWRITE = "overwrite"
    SKIP = "skip"
    IF_CHANGED = "if_changed"


class UploadSymlinks(StrEnum):
    REJECT = "reject"
    PRESERVE_INTERNAL = "preserve_internal"
    FOLLOW_SAFE = "follow_safe"


class UploadSpec(DomainModel):
    source: str | Path
    target: str
    include: tuple[str, ...] = ("**/*",)
    exclude: tuple[str, ...] = ()
    conflict: UploadConflict = UploadConflict.IF_CHANGED
    symlinks: UploadSymlinks = UploadSymlinks.REJECT
    atomic: bool = True
    preserve_mode: bool = False
    preserve_mtime: bool = False
    max_files: int = Field(default=10_000, ge=1)
    max_total_bytes: int = Field(default=512 * 1024 * 1024, ge=1)
    max_file_bytes: int = Field(default=128 * 1024 * 1024, ge=1)


class UploadManifestEntry(DomainModel):
    relative_path: str
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    mode: int = Field(ge=0)
    sha256: str


class UploadManifest(DomainModel):
    upload_id: str
    source_root_hash: str
    target: str
    entries: tuple[UploadManifestEntry, ...]
    manifest_hash: str


class UploadFailure(DomainModel):
    relative_path: str
    error_code: str
    message: str


class UploadResult(DomainModel):
    upload_id: str
    target: str
    total_files: int = Field(ge=0)
    uploaded_files: int = Field(ge=0)
    skipped_files: int = Field(ge=0)
    failed_files: int = Field(ge=0)
    total_bytes: int = Field(ge=0)
    atomic: bool
    manifest_hash: str
    failures: tuple[UploadFailure, ...] = ()
