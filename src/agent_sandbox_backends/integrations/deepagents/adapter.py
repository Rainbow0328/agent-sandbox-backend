from __future__ import annotations

import base64
import fnmatch
import hashlib
from collections.abc import Coroutine
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, TypeVar

from agent_sandbox_backends.application.backend import SandboxBackend
from agent_sandbox_backends.domain.context import ActorContext
from agent_sandbox_backends.domain.errors import (
    FileNotFoundError as BackendFileNotFoundError,
)
from agent_sandbox_backends.domain.errors import SandboxBackendError
from agent_sandbox_backends.domain.files import FileEntry, FileKind
from agent_sandbox_backends.integrations.deepagents.compatibility import (
    FILE_NOT_FOUND,
    INVALID_PATH,
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
    ensure_supported_deepagents,
)
from agent_sandbox_backends.integrations.deepagents.runtime import AsyncRuntimeBridge

ResultT = TypeVar("ResultT")


@dataclass(frozen=True, slots=True)
class DeleteResult:
    error: str | None = None
    path: str | None = None


def as_deepagents_backend(
    backend: SandboxBackend,
    *,
    max_model_output_bytes: int = 500 * 1024,
    runtime: AsyncRuntimeBridge | None = None,
) -> DeepAgentsBackendAdapter:
    """Wrap a core backend with the installed Deep Agents protocol."""
    return DeepAgentsBackendAdapter(
        backend,
        max_model_output_bytes=max_model_output_bytes,
        runtime=runtime,
    )


class DeepAgentsBackendAdapter(SandboxBackendProtocol):
    """Expose a SandboxBackend through the Deep Agents backend protocol."""

    def __init__(
        self,
        backend: SandboxBackend,
        *,
        max_model_output_bytes: int = 500 * 1024,
        runtime: AsyncRuntimeBridge | None = None,
    ) -> None:
        ensure_supported_deepagents()
        if max_model_output_bytes < 1:
            raise ValueError("max_model_output_bytes must be positive")
        self.backend = backend
        self.max_model_output_bytes = max_model_output_bytes
        self._runtime = runtime
        self._owns_runtime = runtime is None

    @property
    def id(self) -> str:
        return self.backend.ref.sandbox_id

    def agent_context(
        self,
        *,
        agent_id: str,
        tool_call_id: str,
        thread_id: str | None = None,
        run_id: str | None = None,
    ) -> AbstractContextManager[ActorContext]:
        return self.backend.agent_context(
            agent_id=agent_id,
            thread_id=thread_id,
            run_id=run_id,
            correlation_id=tool_call_id,
        )

    def ls(self, path: str) -> LsResult:
        return self._sync(self.als(path))

    async def als(self, path: str) -> LsResult:
        try:
            normalized = self._normalize_path(path)
            entries = await self.backend.list_files(normalized)
        except (SandboxBackendError, ValueError) as error:
            return LsResult(error=self._error_message(error))
        return LsResult(entries=[self._file_info(entry) for entry in entries])

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return self._sync(self.aread(file_path, offset, limit))

    async def aread(
        self,
        file_path: str,
        offset: int = 0,
        limit: int = 2000,
    ) -> ReadResult:
        if offset < 0 or limit < 1:
            return ReadResult(error="offset must be non-negative and limit must be positive")
        try:
            normalized = self._normalize_path(file_path)
            content = await self.backend.read_file(normalized)
        except (SandboxBackendError, ValueError) as error:
            return ReadResult(error=self._error_message(error))
        return ReadResult(file_data=self._file_data(content, offset=offset, limit=limit))

    def write(self, file_path: str, content: str) -> WriteResult:
        return self._sync(self.awrite(file_path, content))

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        try:
            normalized = self._normalize_path(file_path)
            try:
                await self.backend.read_file(normalized)
            except BackendFileNotFoundError:
                pass
            else:
                return WriteResult(error=f"File already exists: {normalized}")
            result = await self.backend.write_file(normalized, content.encode("utf-8"))
        except (SandboxBackendError, ValueError) as error:
            return WriteResult(error=self._error_message(error))
        return WriteResult(path=result.entry.path)

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        return self._sync(self.aedit(file_path, old_string, new_string, replace_all))

    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        if old_string == new_string:
            return EditResult(error="old_string and new_string must differ")
        try:
            normalized = self._normalize_path(file_path)
            raw = await self.backend.read_file(normalized)
            text = raw.decode("utf-8")
            occurrences = text.count(old_string)
            if occurrences == 0:
                return EditResult(error=f"String not found in {normalized}")
            if not replace_all and occurrences != 1:
                return EditResult(
                    error=f"String occurs {occurrences} times in {normalized}; set replace_all=True"
                )
            updated = (
                text.replace(old_string, new_string)
                if replace_all
                else text.replace(old_string, new_string, 1)
            )
            expected_hash = self._sha256(raw)
            await self.backend.write_file(
                normalized,
                updated.encode("utf-8"),
                expected_hash=expected_hash,
            )
        except UnicodeDecodeError:
            return EditResult(error=f"File is not UTF-8 text: {file_path}")
        except (SandboxBackendError, ValueError) as error:
            return EditResult(error=self._error_message(error))
        return EditResult(
            path=normalized,
            occurrences=occurrences if replace_all else 1,
        )

    def delete(self, file_path: str) -> DeleteResult:
        return self._sync(self.adelete(file_path))

    async def adelete(self, file_path: str) -> DeleteResult:
        try:
            normalized = self._normalize_path(file_path)
            await self.backend.delete_file(normalized)
        except (SandboxBackendError, ValueError) as error:
            return DeleteResult(error=self._error_message(error))
        return DeleteResult(path=normalized)

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        return self._sync(self.aglob(pattern, path))

    async def aglob(self, pattern: str, path: str | None = None) -> GlobResult:
        try:
            root = self._normalize_path(path or "/")
            entries = await self._walk(root)
        except (SandboxBackendError, ValueError) as error:
            return GlobResult(error=self._error_message(error))
        matches = [
            self._file_info(entry)
            for entry in entries
            if self._glob_matches(entry.path, root, pattern)
        ]
        return GlobResult(matches=matches)

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
    ) -> GrepResult:
        return self._sync(self.agrep(pattern, path, glob))

    async def agrep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
    ) -> GrepResult:
        try:
            root = self._normalize_path(path or "/")
            entries = await self._walk(root)
            matches: list[GrepMatch] = []
            for entry in entries:
                if entry.kind != FileKind.FILE:
                    continue
                if glob is not None and not self._glob_matches(entry.path, root, glob):
                    continue
                raw = await self.backend.read_file(entry.path)
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    continue
                for line_number, line in enumerate(text.splitlines(), 1):
                    if pattern in line:
                        matches.append(
                            {"path": entry.path, "line": line_number, "text": line}
                        )
        except (SandboxBackendError, ValueError) as error:
            return GrepResult(error=self._error_message(error))
        return GrepResult(matches=matches)

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        return self._sync(self.aexecute(command, timeout=timeout))

    async def aexecute(
        self,
        command: str,
        *,
        timeout: int | None = None,  # noqa: ASYNC109
    ) -> ExecuteResponse:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        result = await self.backend.execute(
            command,
            timeout_seconds=float(timeout) if timeout is not None else None,
        )
        output = result.stdout + result.stderr
        rendered, truncated = self._truncate(output)
        return ExecuteResponse(
            output=rendered,
            exit_code=result.exit_code,
            truncated=truncated,
        )

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        return self._sync(self.aupload_files(files))

    async def aupload_files(
        self,
        files: list[tuple[str, bytes]],
    ) -> list[FileUploadResponse]:
        responses: list[FileUploadResponse] = []
        for path, content in files:
            try:
                normalized = self._normalize_path(path)
                await self.backend.write_file(normalized, content)
            except (SandboxBackendError, ValueError) as error:
                responses.append(
                    FileUploadResponse(path=path, error=self._file_operation_error(error))
                )
            else:
                responses.append(FileUploadResponse(path=normalized))
        return responses

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        return self._sync(self.adownload_files(paths))

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        responses: list[FileDownloadResponse] = []
        for path in paths:
            try:
                normalized = self._normalize_path(path)
                content = await self.backend.read_file(normalized)
            except (SandboxBackendError, ValueError) as error:
                responses.append(
                    FileDownloadResponse(path=path, error=self._file_operation_error(error))
                )
            else:
                responses.append(FileDownloadResponse(path=normalized, content=content))
        return responses

    def close(self) -> None:
        if self._runtime is not None and self._owns_runtime:
            self._runtime.close()
            self._runtime = None

    async def aclose(self) -> None:
        self.close()

    async def _walk(self, root: str) -> list[FileEntry]:
        pending = [root]
        seen = {root}
        entries: list[FileEntry] = []
        while pending:
            directory = pending.pop()
            children = await self.backend.list_files(directory)
            entries.extend(children)
            for child in children:
                if child.kind == FileKind.DIRECTORY and child.path not in seen:
                    seen.add(child.path)
                    pending.append(child.path)
        return entries

    def _sync(self, call: Coroutine[Any, Any, ResultT]) -> ResultT:
        if self._runtime is None:
            self._runtime = AsyncRuntimeBridge()
        return self._runtime.run(call)

    @staticmethod
    def _normalize_path(path: str) -> str:
        candidate = PurePosixPath(path)
        if not candidate.is_absolute():
            raise ValueError(f"Deep Agents paths must be absolute: {path}")
        parts: list[str] = []
        for part in candidate.parts:
            if part in {"", "/", "."}:
                continue
            if part == "..":
                raise ValueError("Parent path traversal is not allowed")
            parts.append(part)
        return "/" + "/".join(parts)

    @staticmethod
    def _file_info(entry: FileEntry) -> FileInfo:
        return {
            "path": entry.path,
            "is_dir": entry.kind == FileKind.DIRECTORY,
            "size": entry.size,
        }

    @staticmethod
    def _file_data(content: bytes, *, offset: int, limit: int) -> FileData:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            return {
                "content": base64.b64encode(content).decode("ascii"),
                "encoding": "base64",
            }
        lines = text.splitlines(keepends=True)
        selected = "".join(lines[offset : offset + limit])
        return {"content": selected, "encoding": "utf-8"}

    @staticmethod
    def _glob_matches(path: str, root: str, pattern: str) -> bool:
        relative = path.removeprefix(root.rstrip("/") + "/")
        candidate = relative or PurePosixPath(path).name
        if "/" not in pattern:
            return fnmatch.fnmatch(PurePosixPath(path).name, pattern)
        return fnmatch.fnmatch(candidate, pattern.lstrip("/"))

    def _truncate(self, output: bytes) -> tuple[str, bool]:
        if len(output) <= self.max_model_output_bytes:
            return output.decode("utf-8", errors="replace"), False
        selected = output[: self.max_model_output_bytes]
        rendered = selected.decode("utf-8", errors="replace")
        return rendered + "\n... [output truncated]", True

    @staticmethod
    def _sha256(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _error_message(error: Exception) -> str:
        if isinstance(error, SandboxBackendError):
            return error.message
        return str(error)

    @staticmethod
    def _file_operation_error(error: Exception) -> str:
        if isinstance(error, BackendFileNotFoundError):
            return FILE_NOT_FOUND
        if isinstance(error, ValueError):
            return INVALID_PATH
        if isinstance(error, SandboxBackendError):
            return error.code
        return str(error)
