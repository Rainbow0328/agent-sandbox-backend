from __future__ import annotations

import hashlib
import io
import zipfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class HelperBuildResult:
    path: Path
    sha256: str
    size: int


def build_helper(target: str | Path) -> HelperBuildResult:
    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    content = build_helper_bytes()
    target_path.write_bytes(content)
    return HelperBuildResult(
        path=target_path,
        sha256=hashlib.sha256(content).hexdigest(),
        size=len(content),
    )


def build_helper_bytes() -> bytes:
    source_directory = Path(__file__).parent / "source"
    main_source = b"from history_helper import main\n\nraise SystemExit(main())\n"
    output = io.BytesIO()
    output.write(b"#!/usr/bin/env python3\n")
    with output:
        with zipfile.ZipFile(
            output,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            for name, content in (
                ("__main__.py", main_source),
                ("history_helper.py", (source_directory / "history_helper.py").read_bytes()),
            ):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, content)
        return output.getvalue()


def build_upload_helper_bytes() -> bytes:
    source_directory = Path(__file__).parent / "source"
    main_source = b"from upload_helper import main\n\nraise SystemExit(main())\n"
    output = io.BytesIO()
    output.write(b"#!/usr/bin/env python3\n")
    with output:
        with zipfile.ZipFile(
            output,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            for name, content in (
                ("__main__.py", main_source),
                ("upload_helper.py", (source_directory / "upload_helper.py").read_bytes()),
            ):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, content)
        return output.getvalue()
