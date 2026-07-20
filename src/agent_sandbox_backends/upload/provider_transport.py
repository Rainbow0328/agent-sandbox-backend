from __future__ import annotations

import hashlib
import json
import shlex
from typing import Any, cast

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.domain.commands import CommandState, ExecRequest
from agent_sandbox_backends.domain.errors import UploadPartialFailureError
from agent_sandbox_backends.domain.files import WriteFileRequest
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.uploads import UploadManifest
from agent_sandbox_backends.helper import build_upload_helper_bytes
from agent_sandbox_backends.ports.provider import SandboxProvider

UPLOAD_ROOT = "/.agent-upload"
UPLOAD_HELPER_PATH = f"{UPLOAD_ROOT}/bin/upload-helper-v1.pyz"
UPLOAD_VERSION_PATH = f"{UPLOAD_ROOT}/VERSION"
UPLOAD_TEMP_ROOT = f"{UPLOAD_ROOT}/tmp"


class ProviderUploadArchiveTransport:
    def __init__(
        self,
        provider: SandboxProvider,
        identity: SandboxRef,
        *,
        python_executable: str = "python3",
    ) -> None:
        self.provider = provider
        self.identity = identity
        self.python_executable = python_executable
        self._helper_bytes = build_upload_helper_bytes()
        self._helper_sha256 = hashlib.sha256(self._helper_bytes).hexdigest()

    async def upload_archive(
        self,
        archive: bytes,
        manifest: UploadManifest,
        *,
        target: str,
        conflict: str,
        atomic: bool = False,
    ) -> dict[str, Any]:
        capabilities = await self.provider.capabilities(self.identity)
        capabilities.require("filesystem")
        capabilities.require("command_execution")
        capabilities.require("bulk_upload")
        await self.ensure_installed()
        request_id = str(uuid7())
        staging = f"{UPLOAD_ROOT}/{manifest.upload_id}"
        archive_path = f"{UPLOAD_TEMP_ROOT}/archive-{request_id}.tar.gz"
        manifest_path = f"{UPLOAD_TEMP_ROOT}/manifest-{request_id}.json"
        await self.provider.write_file(
            self.identity,
            WriteFileRequest(path=archive_path, content=archive),
        )
        await self.provider.write_file(
            self.identity,
            WriteFileRequest(
                path=manifest_path,
                content=json.dumps(
                    manifest.model_dump(mode="json"),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"),
            ),
        )
        try:
            await self._invoke(
                "extract",
                "--archive",
                archive_path,
                "--staging",
                staging,
                "--manifest-file",
                manifest_path,
            )
            await self._invoke(
                "verify",
                "--staging",
                staging,
                "--manifest-file",
                manifest_path,
            )
            commit_arguments = [
                "--staging",
                staging,
                "--target",
                target,
                "--manifest-file",
                manifest_path,
                "--conflict",
                conflict,
            ]
            if atomic:
                commit_arguments.append("--atomic")
            return await self._invoke("commit", *commit_arguments)
        finally:
            try:
                await self._invoke("cleanup", "--staging", staging)
            except UploadPartialFailureError:
                pass
            await self._delete_if_present(archive_path)
            await self._delete_if_present(manifest_path)

    async def ensure_installed(self) -> None:
        request_id = str(uuid7())
        helper_temp = f"{UPLOAD_TEMP_ROOT}/helper-{request_id}.pyz"
        await self.provider.write_file(
            self.identity,
            WriteFileRequest(path=helper_temp, content=self._helper_bytes),
        )
        install_script = (
            "import hashlib,os,pathlib,sys;"
            "src,dst,version,expected=sys.argv[1:];"
            "data=pathlib.Path(src).read_bytes();"
            "actual=hashlib.sha256(data).hexdigest();"
            "sys.exit('helper checksum mismatch') if actual!=expected else None;"
            "pathlib.Path(dst).parent.mkdir(parents=True,exist_ok=True);"
            "os.replace(src,dst);"
            "pathlib.Path(version).write_text(expected+'\\n',encoding='utf-8')"
        )
        try:
            result = await self.provider.execute(
                self.identity,
                ExecRequest(
                    command=shlex.join(
                        [
                            self.python_executable,
                            "-c",
                            install_script,
                            helper_temp,
                            UPLOAD_HELPER_PATH,
                            UPLOAD_VERSION_PATH,
                            self._helper_sha256,
                        ]
                    )
                ),
            )
            if result.state != CommandState.SUCCEEDED or result.exit_code != 0:
                raise self._error("upload.install", result.stderr_text or "install failed")
        finally:
            await self._delete_if_present(helper_temp)

    async def _invoke(self, command: str, *arguments: str) -> dict[str, Any]:
        result = await self.provider.execute(
            self.identity,
            ExecRequest(
                command=shlex.join(
                    [
                        self.python_executable,
                        UPLOAD_HELPER_PATH,
                        command,
                        *arguments,
                    ]
                )
            ),
        )
        if result.state != CommandState.SUCCEEDED or result.exit_code != 0:
            raise self._error(
                f"upload.{command}",
                result.stderr_text or "Upload helper command failed",
                retryable=result.state in {CommandState.TIMEOUT, CommandState.UNKNOWN},
            )
        try:
            envelope = json.loads(result.stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise self._error(
                f"upload.{command}",
                "Upload helper returned invalid JSON",
            ) from error
        if not isinstance(envelope, dict):
            raise self._error(f"upload.{command}", "Upload helper response must be an object")
        typed = cast(dict[str, Any], envelope)
        data = typed.get("data")
        if typed.get("ok") is not True or not isinstance(data, dict):
            raise self._error(f"upload.{command}", f"Upload helper rejected request: {typed}")
        return cast(dict[str, Any], data)

    async def _delete_if_present(self, path: str) -> None:
        try:
            await self.provider.delete_file(self.identity, path)
        except Exception:
            pass

    def _error(
        self,
        operation: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> UploadPartialFailureError:
        return UploadPartialFailureError(
            message,
            provider_name=self.identity.provider_name,
            provider_key=self.identity.provider_key,
            sandbox_id=self.identity.sandbox_id,
            sandbox_instance_id=self.identity.sandbox_instance_id,
            operation=operation,
            retryable=retryable,
        )
