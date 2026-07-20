from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, cast

from pathspec import PathSpec

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.config.upload import UploadConfig
from agent_sandbox_backends.domain.errors import (
    LocalPathNotAllowedError,
    LocalPathNotFoundError,
    UploadFileTooLargeError,
    UploadPolicyError,
    UploadSymlinkError,
    UploadTooManyFilesError,
    UploadTotalSizeExceededError,
)
from agent_sandbox_backends.domain.uploads import (
    UploadManifest,
    UploadManifestEntry,
    UploadSpec,
)


@dataclass(frozen=True, slots=True)
class ScannedUpload:
    upload_id: str
    source_root: Path
    source_is_file: bool
    manifest: UploadManifest


class UploadScanner:
    def __init__(self, config: UploadConfig | None = None) -> None:
        self.config = config or UploadConfig()

    def scan(self, spec: UploadSpec, *, upload_id: str | None = None) -> ScannedUpload:
        source = Path(spec.source).expanduser()
        if not source.exists() and not source.is_symlink():
            raise LocalPathNotFoundError("Upload source does not exist")
        source_is_file = source.is_file()
        source_root = source.parent if source_is_file else source
        resolved_root = self._resolve_allowed_root(source_root)
        self._validate_target(spec.target)
        upload_id = upload_id or str(uuid7())
        entries: list[UploadManifestEntry] = []
        total_bytes = 0
        candidates = [source] if source_is_file else self._files(source)
        matcher = self._matcher(spec, source_root)
        for candidate in candidates:
            relative = (
                candidate.name
                if source_is_file
                else candidate.relative_to(source).as_posix()
            )
            if not self._included(relative, matcher):
                continue
            self._validate_local_entry(candidate, resolved_root, spec)
            metadata = candidate.stat()
            if metadata.st_size > spec.max_file_bytes:
                raise UploadFileTooLargeError(
                    f"Upload file exceeds the per-file limit: {relative}",
                    details={"relative_path": relative, "size": metadata.st_size},
                )
            total_bytes += metadata.st_size
            if len(entries) >= spec.max_files:
                raise UploadTooManyFilesError(
                    "Upload contains too many files",
                    details={"max_files": spec.max_files},
                )
            if total_bytes > spec.max_total_bytes:
                raise UploadTotalSizeExceededError(
                    "Upload exceeds the total byte limit",
                    details={"max_total_bytes": spec.max_total_bytes},
                )
            entries.append(
                UploadManifestEntry(
                    relative_path=relative,
                    size=metadata.st_size,
                    mtime_ns=metadata.st_mtime_ns,
                    mode=stat.S_IMODE(metadata.st_mode) if spec.preserve_mode else 0,
                    sha256=self._hash_file(candidate),
                )
            )
        entries.sort(key=lambda entry: entry.relative_path)
        source_root_hash = self._hash_manifest_entries(entries)
        manifest_hash = self._manifest_hash(
            upload_id=upload_id,
            source_root_hash=source_root_hash,
            target=spec.target,
            entries=entries,
        )
        manifest = UploadManifest(
            upload_id=upload_id,
            source_root_hash=source_root_hash,
            target=spec.target,
            entries=tuple(entries),
            manifest_hash=manifest_hash,
        )
        return ScannedUpload(upload_id, resolved_root, source_is_file, manifest)

    def read_entry(self, scanned: ScannedUpload, spec: UploadSpec, relative_path: str) -> bytes:
        source = Path(spec.source).expanduser()
        candidate = source if scanned.source_is_file else source / Path(relative_path)
        return candidate.read_bytes()

    def _resolve_allowed_root(self, source_root: Path) -> Path:
        try:
            resolved = source_root.resolve(strict=True)
        except FileNotFoundError as error:
            raise LocalPathNotFoundError("Upload source root does not exist") from error
        roots = tuple(root.expanduser().resolve() for root in self.config.allowed_local_roots)
        if not roots or not any(self._within(resolved, root) for root in roots):
            raise LocalPathNotAllowedError(
                "Upload source is outside configured allowed_local_roots",
                details={"allowed_root_count": len(roots)},
            )
        return resolved

    def _files(self, source: Path) -> list[Path]:
        result: list[Path] = []
        for root, directories, files in os.walk(source, followlinks=False):
            directories[:] = sorted(directories)
            files = sorted(files)
            for name in files:
                result.append(Path(root) / name)
            for name in directories:
                candidate = Path(root) / name
                if candidate.is_symlink():
                    result.append(candidate)
        return result

    def _matcher(self, spec: UploadSpec, source_root: Path) -> tuple[PathSpec, PathSpec, PathSpec]:
        ignore_lines: list[str] = []
        ignore_file = source_root / ".sandboxignore"
        if ignore_file.is_file():
            ignore_lines = ignore_file.read_text(encoding="utf-8").splitlines()
        include = PathSpec.from_lines("gitwildmatch", spec.include)
        exclude = PathSpec.from_lines("gitwildmatch", spec.exclude)
        defaults = PathSpec.from_lines(
            "gitwildmatch",
            (*self.config.default_excludes, *ignore_lines),
        )
        return include, exclude, defaults

    @staticmethod
    def _included(relative: str, matcher: tuple[PathSpec, PathSpec, PathSpec]) -> bool:
        include, exclude, defaults = matcher
        if not include.match_file(relative):
            return False
        if exclude.match_file(relative):
            return False
        return not defaults.match_file(relative)

    def _validate_local_entry(self, path: Path, root: Path, spec: UploadSpec) -> None:
        if path.is_symlink() or self._is_reparse_point(path):
            if spec.symlinks.value == "reject":
                raise UploadSymlinkError(f"Symlink or reparse point rejected: {path.name}")
            if spec.symlinks.value == "preserve_internal":
                raise UploadPolicyError(
                    "preserve_internal symlinks are not supported by this provider"
                )
            try:
                resolved = path.resolve(strict=True)
            except FileNotFoundError as error:
                raise UploadSymlinkError("Symlink target does not exist") from error
            if not self._within(resolved, root):
                raise UploadSymlinkError("Symlink target escapes the upload source root")

    @staticmethod
    def _is_reparse_point(path: Path) -> bool:
        if os.name != "nt":
            return False
        attributes = cast(int, getattr(path.stat(), "st_file_attributes", 0))
        return bool(attributes & 0x400)

    @staticmethod
    def _within(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
        except ValueError:
            return False
        return True

    @staticmethod
    def _validate_target(target: str) -> None:
        candidate = PurePosixPath(target)
        if not candidate.is_absolute() or ".." in candidate.parts:
            raise UploadPolicyError("Upload target must be an absolute safe POSIX path")

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _hash_manifest_entries(entries: list[UploadManifestEntry]) -> str:
        payload = [entry.model_dump(mode="json") for entry in entries]
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()

    @staticmethod
    def _manifest_hash(
        *,
        upload_id: str,
        source_root_hash: str,
        target: str,
        entries: list[UploadManifestEntry],
    ) -> str:
        payload: dict[str, Any] = {
            "upload_id": upload_id,
            "source_root_hash": source_root_hash,
            "target": target,
            "entries": [entry.model_dump(mode="json") for entry in entries],
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
