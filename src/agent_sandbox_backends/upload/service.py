from __future__ import annotations

import asyncio
import hashlib
import inspect
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from agent_sandbox_backends.config.upload import UploadConfig
from agent_sandbox_backends.domain.errors import (
    FileNotFoundError,
    UploadChecksumMismatchError,
    UploadConflictError,
    UploadPartialFailureError,
    UploadPolicyError,
)
from agent_sandbox_backends.domain.uploads import (
    UploadFailure,
    UploadResult,
    UploadSpec,
)
from agent_sandbox_backends.security.upload_scanner import ScannedUpload, UploadScanner
from agent_sandbox_backends.upload.archive import UploadArchiveBuilder
from agent_sandbox_backends.upload.provider_transport import ProviderUploadArchiveTransport

if TYPE_CHECKING:
    from agent_sandbox_backends.application.backend import SandboxBackend


class UploadService:
    def __init__(
        self,
        backend: SandboxBackend,
        *,
        config: UploadConfig | None = None,
    ) -> None:
        self.backend = backend
        self.config = config or UploadConfig()
        self.scanner = UploadScanner(self.config)

    async def upload(self, spec: UploadSpec) -> UploadResult:
        scanned = self.scanner.scan(spec)
        self._validate_target_roots(spec.target)
        await self._approve(scanned)
        if await self._should_use_archive(scanned):
            return await self._upload_archive(scanned, spec)
        staging_root = f"/.agent-upload/{scanned.upload_id}/files"
        previous: dict[str, bytes | None] = {}
        staged: list[tuple[str, str, bytes]] = []
        failures: list[UploadFailure] = []
        uploaded = 0
        skipped = 0
        try:
            for entry in scanned.manifest.entries:
                target = self._target_path(scanned, spec, entry.relative_path)
                stage = f"{staging_root}/{entry.relative_path}"
                try:
                    if spec.conflict.value == "if_changed":
                        try:
                            current = await self.backend.read_file(target)
                        except FileNotFoundError:
                            current = None
                        if current is not None and self._sha256(current) == entry.sha256:
                            skipped += 1
                            continue
                    content = self.scanner.read_entry(scanned, spec, entry.relative_path)
                    await self.backend.write_file(stage, content)
                    stored = await self.backend.read_file(stage)
                    if self._sha256(stored) != entry.sha256:
                        raise UploadChecksumMismatchError(
                            f"Staging checksum mismatch for {entry.relative_path}"
                        )
                    staged.append((stage, target, content))
                except Exception as error:
                    failure = UploadFailure(
                        relative_path=entry.relative_path,
                        error_code=self._error_code(error),
                        message=self._safe_message(error),
                    )
                    failures.append(failure)
                    if not self.config.partial_success:
                        raise UploadPartialFailureError(
                            "Upload staging failed; all changes were rolled back",
                            details={"failures": [failure.model_dump(mode="json")]},
                        ) from error
            for _, target, content in staged:
                current: bytes | None
                try:
                    current = await self.backend.read_file(target)
                except FileNotFoundError:
                    current = None
                previous.setdefault(target, current)
                action = await self._conflict_action(spec, target, current, content)
                if action == "skip":
                    skipped += 1
                    continue
                await self.backend.write_file(target, content)
                committed = await self.backend.read_file(target)
                if self._sha256(committed) != self._sha256(content):
                    raise UploadChecksumMismatchError(
                        f"Target checksum mismatch for {target}"
                    )
                uploaded += 1
        except Exception:
            await self._rollback(previous)
            raise
        finally:
            await self._cleanup_staging(staged, staging_root)
        return UploadResult(
            upload_id=scanned.upload_id,
            target=spec.target,
            total_files=len(scanned.manifest.entries),
            uploaded_files=uploaded,
            skipped_files=skipped,
            failed_files=len(failures),
            total_bytes=sum(entry.size for entry in scanned.manifest.entries),
            atomic=False,
            manifest_hash=scanned.manifest.manifest_hash,
            failures=tuple(failures),
        )

    async def _should_use_archive(self, scanned: ScannedUpload) -> bool:
        if scanned.source_is_file:
            return False
        if len(scanned.manifest.entries) < self.config.archive_min_files:
            return False
        capabilities = await self.backend.provider.capabilities(self.backend.ref)
        return capabilities.bulk_upload.supported

    async def _upload_archive(
        self,
        scanned: ScannedUpload,
        spec: UploadSpec,
    ) -> UploadResult:
        archive = await asyncio.to_thread(
            UploadArchiveBuilder(self.scanner).build_bytes,
            scanned,
            spec,
        )
        transport = ProviderUploadArchiveTransport(
            self.backend.provider,
            self.backend.ref,
        )
        capabilities = await self.backend.provider.capabilities(self.backend.ref)
        helper_result = await transport.upload_archive(
            archive,
            scanned.manifest,
            target=spec.target,
            conflict=spec.conflict.value,
            atomic=spec.atomic and capabilities.atomic_rename.supported,
        )
        uploaded = int(helper_result.get("committed_files", 0))
        skipped = int(helper_result.get("skipped_files", 0))
        return UploadResult(
            upload_id=scanned.upload_id,
            target=spec.target,
            total_files=len(scanned.manifest.entries),
            uploaded_files=uploaded,
            skipped_files=skipped,
            failed_files=0,
            total_bytes=sum(entry.size for entry in scanned.manifest.entries),
            atomic=bool(helper_result.get("atomic", False)),
            manifest_hash=scanned.manifest.manifest_hash,
        )

    async def _approve(self, scanned: ScannedUpload) -> None:
        callback = self.config.approval_callback
        if callback is None:
            return
        paths = tuple(entry.relative_path for entry in scanned.manifest.entries)
        decision = callback(scanned.manifest.manifest_hash, paths)
        approved = await decision if inspect.isawaitable(decision) else decision
        if not approved:
            raise UploadPolicyError("Upload manifest was rejected by the approval callback")

    async def _conflict_action(
        self,
        spec: UploadSpec,
        target: str,
        current: bytes | None,
        incoming: bytes,
    ) -> str:
        if current is None:
            return "write"
        if spec.conflict.value == "error":
            raise UploadConflictError(f"Upload target already exists: {target}")
        if spec.conflict.value == "skip":
            return "skip"
        if spec.conflict.value == "if_changed" and current == incoming:
            return "skip"
        return "write"

    async def _rollback(self, previous: dict[str, bytes | None]) -> None:
        for target, content in reversed(tuple(previous.items())):
            try:
                if content is None:
                    await self.backend.delete_file(target)
                else:
                    await self.backend.write_file(target, content)
            except Exception:
                continue

    async def _cleanup_staging(self, staged: list[tuple[str, str, bytes]], root: str) -> None:
        for stage, _, _ in reversed(staged):
            try:
                await self.backend.delete_file(stage)
            except Exception:
                continue
        del root

    def _validate_target_roots(self, target: str) -> None:
        candidate = PurePosixPath(target)
        if not candidate.is_absolute() or ".." in candidate.parts:
            raise UploadPolicyError("Upload target must be an absolute safe POSIX path")
        normalized = "/" + "/".join(part for part in candidate.parts if part != "/")
        if any(
            normalized == protected or normalized.startswith(protected.rstrip("/") + "/")
            for protected in self.config.protected_sandbox_roots
        ):
            raise UploadPolicyError("Upload target is protected")
        if not any(
            normalized == allowed or normalized.startswith(allowed.rstrip("/") + "/")
            for allowed in self.config.allowed_sandbox_roots
        ):
            raise UploadPolicyError("Upload target is outside allowed_sandbox_roots")

    @staticmethod
    def _target_path(scanned: ScannedUpload, spec: UploadSpec, relative: str) -> str:
        if scanned.source_is_file:
            return spec.target
        return str(PurePosixPath(spec.target) / relative)

    @staticmethod
    def _sha256(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _error_code(error: Exception) -> str:
        return getattr(error, "code", type(error).__name__)

    @staticmethod
    def _safe_message(error: Exception) -> str:
        return getattr(error, "message", str(error))
