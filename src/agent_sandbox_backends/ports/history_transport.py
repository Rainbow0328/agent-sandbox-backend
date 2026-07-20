from __future__ import annotations

from typing import Any, Protocol

from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.history.config import HistoryConfig


class HistoryHelperTransport(Protocol):
    identity: SandboxRef

    async def ensure_installed(
        self,
        *,
        sdk_version: str,
        config: HistoryConfig,
    ) -> dict[str, Any]: ...

    async def invoke(
        self,
        command: str,
        *,
        arguments: tuple[str, ...] = (),
        payload: dict[str, Any] | None = None,
        payload_flag: str | None = None,
    ) -> dict[str, Any]: ...
