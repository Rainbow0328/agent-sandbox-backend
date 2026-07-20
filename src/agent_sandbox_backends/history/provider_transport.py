from __future__ import annotations

import hashlib
import json
import shlex
from typing import Any, cast

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.domain.commands import CommandState, ExecRequest
from agent_sandbox_backends.domain.errors import HistoryTransportError
from agent_sandbox_backends.domain.files import WriteFileRequest
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.sandbox import SANDBOX_NAME_METADATA_KEY
from agent_sandbox_backends.helper import build_helper_bytes
from agent_sandbox_backends.helper.source.history_helper import canonical_json
from agent_sandbox_backends.history.config import HistoryConfig
from agent_sandbox_backends.ports.provider import SandboxProvider

HISTORY_ROOT = "/.agent-history"
HISTORY_HELPER_PATH = f"{HISTORY_ROOT}/bin/history-helper-v1.pyz"
HISTORY_DATABASE_PATH = f"{HISTORY_ROOT}/history.sqlite3"
HISTORY_TEMP_ROOT = f"{HISTORY_ROOT}/tmp"
HISTORY_VERSION_PATH = f"{HISTORY_ROOT}/VERSION"


class ProviderHistoryHelperTransport:
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
        self._helper_bytes = build_helper_bytes()
        self._helper_sha256 = hashlib.sha256(self._helper_bytes).hexdigest()

    async def ensure_installed(
        self,
        *,
        sdk_version: str,
        config: HistoryConfig,
    ) -> dict[str, Any]:
        capabilities = await self.provider.capabilities(self.identity)
        capabilities.require("filesystem")
        capabilities.require("command_execution")

        request_id = str(uuid7())
        helper_temp = f"{HISTORY_TEMP_ROOT}/helper-{request_id}.pyz"
        identity_temp = f"{HISTORY_TEMP_ROOT}/identity-{request_id}.json"
        await self.provider.write_file(
            self.identity,
            WriteFileRequest(path=helper_temp, content=self._helper_bytes),
        )
        try:
            await self._install_helper(helper_temp)
            identity_payload = {
                "provider_key": self.identity.provider_key,
                "sandbox_id": self.identity.sandbox_id,
                "sandbox_instance_id": self.identity.sandbox_instance_id,
                "sdk_version": sdk_version,
                "schema_version": 1,
                "history_config": config.persisted_values(),
            }
            sandbox_name = (
                self.identity.metadata.get(SANDBOX_NAME_METADATA_KEY)
                or self.identity.metadata.get("sandbox_name")
                or ""
            ).strip()
            if sandbox_name:
                identity_payload["sandbox_name"] = sandbox_name
                identity_payload["sandbox_metadata"] = {
                    SANDBOX_NAME_METADATA_KEY: sandbox_name,
                }
            await self.provider.write_file(
                self.identity,
                WriteFileRequest(
                    path=identity_temp,
                    content=canonical_json(identity_payload).encode("utf-8"),
                ),
            )
            await self.invoke(
                "init",
                arguments=("--identity-file", identity_temp),
            )
            return await self.invoke("health")
        finally:
            await self._delete_if_present(helper_temp)
            await self._delete_if_present(identity_temp)

    async def invoke(
        self,
        command: str,
        *,
        arguments: tuple[str, ...] = (),
        payload: dict[str, Any] | None = None,
        payload_flag: str | None = None,
    ) -> dict[str, Any]:
        payload_path: str | None = None
        if payload is not None:
            if payload_flag is None:
                raise ValueError("payload_flag is required when payload is provided")
            payload_path = f"{HISTORY_TEMP_ROOT}/request-{uuid7()}.json"
            await self.provider.write_file(
                self.identity,
                WriteFileRequest(
                    path=payload_path,
                    content=canonical_json(payload).encode("utf-8"),
                ),
            )

        command_arguments = [
            self.python_executable,
            HISTORY_HELPER_PATH,
            "--database",
            HISTORY_DATABASE_PATH,
            command,
            *arguments,
        ]
        if payload_path is not None and payload_flag is not None:
            command_arguments.extend((payload_flag, payload_path))

        try:
            result = await self.provider.execute(
                self.identity,
                ExecRequest(command=shlex.join(command_arguments)),
            )
            return self._parse_result(
                command,
                result.state,
                result.exit_code,
                result.stdout,
                result.stderr,
            )
        finally:
            if payload_path is not None:
                await self._delete_if_present(payload_path)

    async def _install_helper(self, helper_temp: str) -> None:
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
        result = await self.provider.execute(
            self.identity,
            ExecRequest(
                command=shlex.join(
                    [
                        self.python_executable,
                        "-c",
                        install_script,
                        helper_temp,
                        HISTORY_HELPER_PATH,
                        HISTORY_VERSION_PATH,
                        self._helper_sha256,
                    ]
                )
            ),
        )
        if result.state != CommandState.SUCCEEDED or result.exit_code != 0:
            raise self._transport_error(
                "history.install",
                f"Helper install failed: {result.stderr_text}",
                retryable=False,
            )

    def _parse_result(
        self,
        command: str,
        state: CommandState,
        exit_code: int | None,
        stdout: bytes,
        stderr: bytes,
    ) -> dict[str, Any]:
        if state != CommandState.SUCCEEDED or exit_code != 0:
            raise self._transport_error(
                f"history.{command}",
                stderr.decode("utf-8", errors="replace") or "History helper command failed",
                retryable=state in {CommandState.TIMEOUT, CommandState.UNKNOWN},
            )
        try:
            envelope = json.loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise self._transport_error(
                f"history.{command}",
                "History helper returned invalid JSON",
                retryable=False,
            ) from error
        if not isinstance(envelope, dict):
            raise self._transport_error(
                f"history.{command}",
                "History helper response must be an object",
                retryable=False,
            )
        typed_envelope = cast(dict[str, Any], envelope)
        if typed_envelope.get("ok") is not True:
            raise self._transport_error(
                f"history.{command}",
                f"History helper rejected request: {typed_envelope}",
                retryable=False,
            )
        data = typed_envelope.get("data")
        if not isinstance(data, dict):
            raise self._transport_error(
                f"history.{command}",
                "History helper response is missing data object",
                retryable=False,
            )
        return cast(dict[str, Any], data)

    async def _delete_if_present(self, path: str) -> None:
        try:
            await self.provider.delete_file(self.identity, path)
        except Exception:
            return

    def _transport_error(
        self,
        operation: str,
        message: str,
        *,
        retryable: bool,
    ) -> HistoryTransportError:
        return HistoryTransportError(
            message,
            provider_name=self.identity.provider_name,
            provider_key=self.identity.provider_key,
            sandbox_id=self.identity.sandbox_id,
            sandbox_instance_id=self.identity.sandbox_instance_id,
            operation=operation,
            retryable=retryable,
        )
