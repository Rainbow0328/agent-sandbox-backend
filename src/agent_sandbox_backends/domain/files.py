from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from agent_sandbox_backends.domain.base import DomainModel


class FileKind(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    OTHER = "other"


class FileEntry(DomainModel):
    path: str
    kind: FileKind
    size: int = Field(ge=0)
    content_hash: str | None = None


class WriteFileRequest(DomainModel):
    path: str
    content: bytes
    expected_hash: str | None = None


class WriteFileResult(DomainModel):
    entry: FileEntry
    previous_hash: str | None = None
    cas_strength: str = "native"
