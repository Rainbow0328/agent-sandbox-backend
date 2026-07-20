from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tarfile
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, cast


class UploadHelperError(RuntimeError):
    pass


def extract_verified(
    archive_path: str | os.PathLike[str],
    staging_root: str | os.PathLike[str],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    root = Path(staging_root)
    root.mkdir(parents=True, exist_ok=True)
    root_resolved = root.resolve()
    expected = _manifest_entries(manifest)
    extracted: set[str] = set()
    total_bytes = 0
    with tarfile.open(archive_path, mode="r:gz") as archive:
        members = archive.getmembers()
        if len(members) != len(expected):
            raise UploadHelperError("Archive file count does not match manifest")
        for member in members:
            relative = _safe_relative(member.name)
            entry = expected.get(relative)
            if entry is None:
                raise UploadHelperError(f"Archive entry is absent from manifest: {relative}")
            if not member.isfile() or member.issym() or member.islnk() or member.isdev():
                raise UploadHelperError("Archive contains an unsupported entry type")
            size = int(entry["size"])
            if member.size != size:
                raise UploadHelperError(f"Archive size mismatch: {relative}")
            total_bytes += member.size
            target = root / Path(relative)
            _require_within(target, root_resolved)
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise UploadHelperError(f"Cannot read archive entry: {relative}")
            temporary = target.with_name(f".{target.name}.uploading")
            digest = hashlib.sha256()
            with temporary.open("wb") as output:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
                    digest.update(chunk)
                output.flush()
                os.fsync(output.fileno())
            if digest.hexdigest() != str(entry["sha256"]):
                temporary.unlink(missing_ok=True)
                raise UploadHelperError(f"Archive checksum mismatch: {relative}")
            os.replace(temporary, target)
            mode = int(entry.get("mode", 0)) & 0o777 & ~0o6000
            if mode:
                target.chmod(mode)
            extracted.add(relative)
    missing = sorted(set(expected) - extracted)
    if missing:
        raise UploadHelperError(f"Manifest entries were not extracted: {', '.join(missing)}")
    return {
        "ok": True,
        "file_count": len(extracted),
        "total_bytes": total_bytes,
        "manifest_hash": manifest.get("manifest_hash"),
    }


def verify_staging(
    staging_root: str | os.PathLike[str],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    root = Path(staging_root).resolve()
    expected = _manifest_entries(manifest)
    total_bytes = 0
    for relative, entry in expected.items():
        path = root / Path(relative)
        _require_within(path, root)
        if not path.is_file() or path.is_symlink():
            raise UploadHelperError(f"Staging file is missing or unsafe: {relative}")
        size = path.stat().st_size
        if size != int(entry["size"]):
            raise UploadHelperError(f"Staging size mismatch: {relative}")
        if _hash_file(path) != str(entry["sha256"]):
            raise UploadHelperError(f"Staging checksum mismatch: {relative}")
        total_bytes += size
    return {"ok": True, "file_count": len(expected), "total_bytes": total_bytes}


def commit_staging(
    staging_root: str | os.PathLike[str],
    target_root: str | os.PathLike[str],
    manifest: dict[str, Any],
    *,
    conflict: str,
    atomic: bool = False,
) -> dict[str, Any]:
    if conflict not in {"error", "overwrite", "skip", "if_changed"}:
        raise UploadHelperError("Unsupported conflict policy")
    verify_staging(staging_root, manifest)
    staging = Path(staging_root).resolve()
    target = Path(target_root).resolve()
    expected = _manifest_entries(manifest)
    if atomic and conflict in {"error", "overwrite"}:
        return _commit_staging_atomic(staging, target, expected, conflict=conflict)
    actions: list[tuple[str, Path, Path]] = []
    skipped = 0
    for relative, entry in expected.items():
        source = staging / Path(relative)
        destination = target / Path(relative)
        _require_within(source, staging)
        _require_within(destination, target)
        if destination.exists():
            if destination.is_symlink() or not destination.is_file():
                raise UploadHelperError(f"Unsafe upload conflict: {relative}")
            if conflict == "error":
                raise UploadHelperError(f"Upload target already exists: {relative}")
            if conflict == "skip":
                skipped += 1
                continue
            if conflict == "if_changed" and _hash_file(destination) == str(entry["sha256"]):
                skipped += 1
                continue
        actions.append((relative, source, destination))

    rollback = staging / ".rollback"
    journal: list[tuple[Path, Path | None]] = []
    committed = 0
    try:
        for relative, source, destination in actions:
            destination.parent.mkdir(parents=True, exist_ok=True)
            backup: Path | None = None
            if destination.exists():
                backup = rollback / Path(relative)
                backup.parent.mkdir(parents=True, exist_ok=True)
                os.replace(destination, backup)
            temporary = destination.with_name(f".{destination.name}.uploading")
            shutil.copyfile(source, temporary)
            with temporary.open("rb+") as handle:
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
            journal.append((destination, backup))
            committed += 1
    except Exception as error:
        for destination, backup in reversed(journal):
            destination.unlink(missing_ok=True)
            if backup is not None and backup.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(backup, destination)
        raise UploadHelperError("Upload commit failed and was rolled back") from error
    finally:
        shutil.rmtree(rollback, ignore_errors=True)
    return {
        "ok": True,
        "committed_files": committed,
        "skipped_files": skipped,
        "atomic": False,
    }


def _commit_staging_atomic(
    staging: Path,
    target: Path,
    expected: dict[str, dict[str, Any]],
    *,
    conflict: str,
) -> dict[str, Any]:
    if target.is_symlink():
        raise UploadHelperError("Atomic upload target cannot be a symlink")
    if target.exists() and conflict == "error":
        raise UploadHelperError("Atomic upload target already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = target.with_name(f".{target.name}.upload-backup-{uuid.uuid4().hex}")
    moved_target = False
    try:
        if target.exists():
            os.replace(target, backup)
            moved_target = True
        os.replace(staging, target)
    except Exception as error:
        if moved_target and backup.exists() and not target.exists():
            os.replace(backup, target)
        raise UploadHelperError("Atomic upload commit failed and was rolled back") from error
    finally:
        if backup.exists() and target.exists():
            if backup.is_dir():
                shutil.rmtree(backup, ignore_errors=True)
            else:
                backup.unlink(missing_ok=True)
    return {
        "ok": True,
        "committed_files": len(expected),
        "skipped_files": 0,
        "atomic": True,
    }


def cleanup_staging(staging_root: str | os.PathLike[str]) -> dict[str, Any]:
    root = Path(staging_root)
    shutil.rmtree(root, ignore_errors=True)
    return {"ok": True, "removed": not root.exists()}


def _manifest_entries(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_entries = manifest.get("entries")
    if not isinstance(raw_entries, list):
        raise UploadHelperError("Manifest entries must be a list")
    entries: dict[str, dict[str, Any]] = {}
    for raw_item in cast(list[object], raw_entries):
        if not isinstance(raw_item, dict):
            raise UploadHelperError("Manifest entry must be an object")
        item = cast(dict[str, Any], raw_item)
        relative = _safe_relative(str(item.get("relative_path", "")))
        if relative in entries:
            raise UploadHelperError(f"Duplicate manifest entry: {relative}")
        entries[relative] = item
    return entries


def _safe_relative(value: str) -> str:
    normalized = value.replace("\\", "/")
    candidate = PurePosixPath(normalized)
    if not normalized or candidate.is_absolute() or ".." in candidate.parts:
        raise UploadHelperError("Path escapes staging root")
    return candidate.as_posix()


def _require_within(path: Path, root: Path) -> None:
    resolved_parent = path.parent.resolve()
    try:
        resolved_parent.relative_to(root)
    except ValueError as error:
        raise UploadHelperError("Resolved path escapes staging root") from error


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="upload-helper")
    subcommands = parser.add_subparsers(dest="command", required=True)
    extract = subcommands.add_parser("extract")
    extract.add_argument("--archive", required=True)
    extract.add_argument("--staging", required=True)
    extract.add_argument("--manifest-file", required=True)
    verify = subcommands.add_parser("verify")
    verify.add_argument("--staging", required=True)
    verify.add_argument("--manifest-file", required=True)
    commit = subcommands.add_parser("commit")
    commit.add_argument("--staging", required=True)
    commit.add_argument("--target", required=True)
    commit.add_argument("--manifest-file", required=True)
    commit.add_argument("--conflict", required=True)
    commit.add_argument("--atomic", action="store_true")
    cleanup = subcommands.add_parser("cleanup")
    cleanup.add_argument("--staging", required=True)
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "cleanup":
            data = cleanup_staging(arguments.staging)
        else:
            manifest = json.loads(Path(arguments.manifest_file).read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                raise UploadHelperError("Manifest root must be an object")
            typed_manifest = cast(dict[str, Any], manifest)
            if arguments.command == "extract":
                data = extract_verified(arguments.archive, arguments.staging, typed_manifest)
            elif arguments.command == "verify":
                data = verify_staging(arguments.staging, typed_manifest)
            else:
                data = commit_staging(
                    arguments.staging,
                    arguments.target,
                    typed_manifest,
                    conflict=arguments.conflict,
                    atomic=arguments.atomic,
                )
        print(json.dumps({"ok": True, "data": data}, separators=(",", ":"), sort_keys=True))
        return 0
    except (OSError, tarfile.TarError, UploadHelperError, ValueError) as error:
        print(
            json.dumps(
                {"ok": False, "error": type(error).__name__, "message": str(error)},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
