from __future__ import annotations

import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from agent_sandbox_backends.domain.errors import UploadArchiveSecurityError


@dataclass(frozen=True, slots=True)
class ArchiveLimits:
    max_files: int = 10_000
    max_file_bytes: int = 128 * 1024 * 1024
    max_total_bytes: int = 512 * 1024 * 1024
    max_compression_ratio: float = 200


@dataclass(frozen=True, slots=True)
class ArchiveSummary:
    file_count: int
    total_bytes: int


def validate_archive(
    archive: str | Path,
    *,
    limits: ArchiveLimits | None = None,
) -> ArchiveSummary:
    path = Path(archive)
    resolved_limits = limits or ArchiveLimits()
    if tarfile.is_tarfile(path):
        return _validate_tar(path, resolved_limits)
    if zipfile.is_zipfile(path):
        return _validate_zip(path, resolved_limits)
    raise UploadArchiveSecurityError("Unsupported or invalid archive format")


def _validate_tar(path: Path, limits: ArchiveLimits) -> ArchiveSummary:
    file_count = 0
    total_bytes = 0
    with tarfile.open(path, mode="r:*") as archive:
        for member in archive.getmembers():
            _validate_entry_name(member.name)
            if member.issym() or member.islnk():
                raise UploadArchiveSecurityError("Archive links are not allowed")
            if member.isdev() or member.isfifo():
                raise UploadArchiveSecurityError("Archive special files are not allowed")
            if member.isdir():
                continue
            if not member.isfile():
                raise UploadArchiveSecurityError("Unsupported tar entry type")
            file_count += 1
            total_bytes += member.size
            _validate_limits(file_count, member.size, total_bytes, path, limits)
    return ArchiveSummary(file_count=file_count, total_bytes=total_bytes)


def _validate_zip(path: Path, limits: ArchiveLimits) -> ArchiveSummary:
    file_count = 0
    total_bytes = 0
    compressed_bytes = 0
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            _validate_entry_name(member.filename)
            if member.is_dir():
                continue
            unix_mode = member.external_attr >> 16
            if unix_mode and (unix_mode & 0o170000) == 0o120000:
                raise UploadArchiveSecurityError("Archive symlinks are not allowed")
            file_count += 1
            total_bytes += member.file_size
            compressed_bytes += member.compress_size
            _validate_limits(file_count, member.file_size, total_bytes, path, limits)
    ratio = total_bytes / max(compressed_bytes, 1)
    if ratio > limits.max_compression_ratio:
        raise UploadArchiveSecurityError(
            "Archive compression ratio exceeds the configured limit",
            details={"compression_ratio": ratio},
        )
    return ArchiveSummary(file_count=file_count, total_bytes=total_bytes)


def _validate_entry_name(name: str) -> None:
    normalized = name.replace("\\", "/")
    candidate = PurePosixPath(normalized)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise UploadArchiveSecurityError("Archive entry escapes the staging root")
    if not normalized or normalized.startswith(("//", "\\\\")):
        raise UploadArchiveSecurityError("Archive entry has an invalid path")


def _validate_limits(
    file_count: int,
    file_bytes: int,
    total_bytes: int,
    archive: Path,
    limits: ArchiveLimits,
) -> None:
    if file_count > limits.max_files:
        raise UploadArchiveSecurityError("Archive contains too many files")
    if file_bytes > limits.max_file_bytes:
        raise UploadArchiveSecurityError("Archive entry exceeds the file size limit")
    if total_bytes > limits.max_total_bytes:
        raise UploadArchiveSecurityError("Archive exceeds the total size limit")
    archive_bytes = max(archive.stat().st_size, 1)
    if total_bytes / archive_bytes > limits.max_compression_ratio:
        raise UploadArchiveSecurityError(
            "Archive expansion ratio exceeds the configured limit"
        )
