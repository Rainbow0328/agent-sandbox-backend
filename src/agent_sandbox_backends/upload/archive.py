from __future__ import annotations

import gzip
import hashlib
import io
import tarfile
from dataclasses import dataclass
from pathlib import Path

from agent_sandbox_backends.domain.uploads import UploadSpec
from agent_sandbox_backends.security.upload_scanner import ScannedUpload, UploadScanner


@dataclass(frozen=True, slots=True)
class UploadArchiveArtifact:
    path: Path
    size: int
    sha256: str
    manifest_hash: str


class UploadArchiveBuilder:
    """Build deterministic tar.gz archives exclusively from a scanned manifest."""

    def __init__(self, scanner: UploadScanner) -> None:
        self.scanner = scanner

    def build(
        self,
        scanned: ScannedUpload,
        spec: UploadSpec,
        destination: str | Path,
    ) -> UploadArchiveArtifact:
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        content = self.build_bytes(scanned, spec)
        target.write_bytes(content)
        return UploadArchiveArtifact(
            path=target,
            size=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            manifest_hash=scanned.manifest.manifest_hash,
        )

    def build_bytes(self, scanned: ScannedUpload, spec: UploadSpec) -> bytes:
        output = io.BytesIO()
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.PAX_FORMAT,
            ) as archive:
                for entry in scanned.manifest.entries:
                    content = self.scanner.read_entry(
                        scanned,
                        spec,
                        entry.relative_path,
                    )
                    if hashlib.sha256(content).hexdigest() != entry.sha256:
                        raise ValueError(
                            f"Local file changed after manifest scan: {entry.relative_path}"
                        )
                    info = tarfile.TarInfo(entry.relative_path)
                    info.size = len(content)
                    info.mtime = entry.mtime_ns // 1_000_000_000 if spec.preserve_mtime else 0
                    info.mode = self._safe_mode(entry.mode, preserve=spec.preserve_mode)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    archive.addfile(info, io.BytesIO(content))
        return output.getvalue()

    @staticmethod
    def _safe_mode(mode: int, *, preserve: bool) -> int:
        selected = mode if preserve else 0o644
        return selected & 0o777 & ~0o6000
