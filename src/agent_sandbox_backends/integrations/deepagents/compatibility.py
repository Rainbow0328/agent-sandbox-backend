from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    from deepagents.backends.protocol import (
        FILE_NOT_FOUND,
        INVALID_PATH,
        BackendProtocol,
        EditResult,
        ExecuteResponse,
        FileData,
        FileDownloadResponse,
        FileInfo,
        FileUploadResponse,
        GlobResult,
        GrepMatch,
        GrepResult,
        LsResult,
        ReadResult,
        SandboxBackendProtocol,
        WriteResult,
    )
except ImportError as error:
    raise ImportError(
        "Deep Agents integration requires the 'deepagents' extra: "
        "pip install 'agent-sandbox-backends[deepagents]'"
    ) from error

MINIMUM_VERSION = (0, 6, 12)
MAXIMUM_VERSION = (0, 8, 0)


class DeepAgentsCompatibilityError(RuntimeError):
    """Raised when the installed Deep Agents version is unsupported."""


def deepagents_version() -> str:
    try:
        return version("deepagents")
    except PackageNotFoundError as error:
        raise DeepAgentsCompatibilityError("Deep Agents is not installed") from error


def ensure_supported_deepagents() -> str:
    installed = deepagents_version()
    parsed = _release_tuple(installed)
    if parsed < MINIMUM_VERSION or parsed >= MAXIMUM_VERSION:
        supported = ">=0.6.12,<0.8"
        raise DeepAgentsCompatibilityError(
            f"Unsupported Deep Agents version {installed}; expected {supported}"
        )
    return installed


def _release_tuple(value: str) -> tuple[int, int, int]:
    release = value.split("+", 1)[0].split("-", 1)[0]
    parts = release.split(".")
    numeric: list[int] = []
    for part in parts[:3]:
        digits = "".join(character for character in part if character.isdigit())
        numeric.append(int(digits or 0))
    while len(numeric) < 3:
        numeric.append(0)
    return numeric[0], numeric[1], numeric[2]


__all__ = [
    "FILE_NOT_FOUND",
    "INVALID_PATH",
    "BackendProtocol",
    "DeepAgentsCompatibilityError",
    "EditResult",
    "ExecuteResponse",
    "FileData",
    "FileDownloadResponse",
    "FileInfo",
    "FileUploadResponse",
    "GlobResult",
    "GrepMatch",
    "GrepResult",
    "LsResult",
    "ReadResult",
    "SandboxBackendProtocol",
    "WriteResult",
    "deepagents_version",
    "ensure_supported_deepagents",
]
