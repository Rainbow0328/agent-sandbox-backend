from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel

UploadApprovalCallback = Callable[[str, tuple[str, ...]], Awaitable[bool] | bool]


class UploadConfig(DomainModel):
    allowed_local_roots: tuple[Path, ...] = ()
    allowed_sandbox_roots: tuple[str, ...] = ("/workspace",)
    protected_sandbox_roots: tuple[str, ...] = (
        "/.agent-history",
        "/.agent-upload",
    )
    default_excludes: tuple[str, ...] = (
        ".git/",
        ".env",
        ".env.*",
        ".ssh/",
        ".aws/",
        ".gnupg/",
        "node_modules/",
        ".venv/",
        "__pycache__/",
        ".agent-history/",
        ".sandboxignore",
    )
    max_parallel_files: int = Field(default=8, ge=1, le=128)
    archive_min_files: int = Field(default=64, ge=1)
    partial_success: bool = False
    redact_local_paths: bool = True
    approval_callback: UploadApprovalCallback | None = None
