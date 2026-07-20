from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.application.allocator import SandboxAllocator
from agent_sandbox_backends.application.backend import SandboxBackend
from agent_sandbox_backends.application.operation_pipeline import OperationPipeline
from agent_sandbox_backends.config.command import CommandResultConfig
from agent_sandbox_backends.config.concurrency import ConcurrencyConfig, SandboxSharingMode
from agent_sandbox_backends.config.models import BackendMode, CleanupPolicy
from agent_sandbox_backends.config.retry import RetryConfig
from agent_sandbox_backends.config.upload import UploadConfig
from agent_sandbox_backends.domain.identity import SandboxRef
from agent_sandbox_backends.domain.sandbox import (
    SANDBOX_NAME_METADATA_KEY,
    CreateSandboxRequest,
)
from agent_sandbox_backends.domain.uploads import UploadSpec
from agent_sandbox_backends.history.bootstrap_outbox import BootstrapOutbox
from agent_sandbox_backends.history.config import (
    HistoryConfig,
    HistoryConsistency,
    HistoryMode,
)
from agent_sandbox_backends.history.memory import MemoryHistoryStore
from agent_sandbox_backends.history.none import NoneHistoryStore
from agent_sandbox_backends.history.provider_transport import ProviderHistoryHelperTransport
from agent_sandbox_backends.history.sandbox import SandboxHistoryStore
from agent_sandbox_backends.ports.history_store import HistoryStore
from agent_sandbox_backends.ports.history_transport import HistoryHelperTransport
from agent_sandbox_backends.ports.provider import SandboxProvider
from agent_sandbox_backends.providers.registry import ProviderRegistry
from agent_sandbox_backends.version import SDK_VERSION

if TYPE_CHECKING:
    from agent_sandbox_backends.history.sqlalchemy import SQLAlchemyHistoryStore

HistoryTransportFactory = Callable[[SandboxProvider, SandboxRef], HistoryHelperTransport]


async def create_opensandbox_backend(
    url: str,
    *,
    api_key: str | None = None,
    sandbox_name: str | None = None,
    mode: BackendMode | str = BackendMode.CREATE,
    ref: SandboxRef | None = None,
    image: str = "python:3.12",
    workdir: str = "/workspace",
    env: dict[str, str] | None = None,
    metadata: dict[str, str] | None = None,
    idempotency_key: str | None = None,
    cleanup: CleanupPolicy | str = CleanupPolicy.ON_CLOSE,
    sandbox_ttl_seconds: float | None = None,
    cleanup_ttl_seconds: float | None = None,
    use_server_proxy: bool = True,
    request_timeout_seconds: float = 30,
    ready_timeout_seconds: float = 30,
    provider_key: str = "opensandbox-default",
    headers: dict[str, str] | None = None,
    debug: bool = False,
    concurrency: ConcurrencyConfig | None = None,
    command_result_config: CommandResultConfig | None = None,
    retry: RetryConfig | None = None,
    uploads: Sequence[UploadSpec] = (),
    upload_config: UploadConfig | None = None,
    history: HistoryConfig | None = None,
) -> SandboxBackend:
    """Create an OpenSandbox-backed Backend with first-release defaults.

    The API key is optional, Python 3.12 is the default image, and history is
    persisted in SQLite inside the sandbox unless explicitly disabled. When
    sandbox_ttl_seconds is omitted, the OpenSandbox SDK default is preserved.
    """
    protocol, domain = _parse_opensandbox_url(url)
    resolved_metadata = _metadata_with_sandbox_name(
        metadata,
        sandbox_name=sandbox_name,
        mode=mode,
    )
    resolved_history = history or HistoryConfig(mode=HistoryMode.SANDBOX)
    return await create_backend(
        provider="opensandbox",
        mode=mode,
        ref=ref,
        image=image,
        workdir=workdir,
        env=env,
        metadata=resolved_metadata,
        idempotency_key=idempotency_key,
        cleanup=cleanup,
        sandbox_ttl_seconds=sandbox_ttl_seconds,
        cleanup_ttl_seconds=cleanup_ttl_seconds,
        concurrency=concurrency,
        command_result_config=command_result_config,
        retry=retry,
        uploads=uploads,
        upload_config=upload_config,
        history=resolved_history,
        provider_options={
            "provider_key": provider_key,
            "api_key": api_key,
            "domain": domain,
            "protocol": protocol,
            "request_timeout_seconds": request_timeout_seconds,
            "ready_timeout_seconds": ready_timeout_seconds,
            "debug": debug,
            "headers": headers,
            "use_server_proxy": use_server_proxy,
        },
    )


async def create_backend(
    *,
    provider: str | SandboxProvider,
    mode: BackendMode | str = BackendMode.CREATE,
    ref: SandboxRef | None = None,
    image: str = "python:3.12",
    workdir: str = "/workspace",
    env: dict[str, str] | None = None,
    metadata: dict[str, str] | None = None,
    idempotency_key: str | None = None,
    cleanup: CleanupPolicy | str = CleanupPolicy.ON_CLOSE,
    sandbox_ttl_seconds: float | None = None,
    cleanup_ttl_seconds: float | None = None,
    concurrency: ConcurrencyConfig | None = None,
    command_result_config: CommandResultConfig | None = None,
    retry: RetryConfig | None = None,
    uploads: Sequence[UploadSpec] = (),
    upload_config: UploadConfig | None = None,
    history: HistoryConfig | None = None,
    history_store: HistoryStore | None = None,
    history_database_url: str | None = None,
    bootstrap_outbox: BootstrapOutbox | None = None,
    history_transport_factory: HistoryTransportFactory | None = None,
    registry: ProviderRegistry | None = None,
    provider_options: dict[str, Any] | None = None,
    close_coordinator: Callable[
        [SandboxBackend, Callable[[], Awaitable[None]]], Awaitable[None]
    ]
    | None = None,
) -> SandboxBackend:
    owns_provider = isinstance(provider, str)
    owns_history_store = history_store is None
    resolved_mode = BackendMode(mode)
    resolved_cleanup = CleanupPolicy(cleanup)
    if sandbox_ttl_seconds is not None and sandbox_ttl_seconds <= 0:
        raise ValueError("sandbox_ttl_seconds must be greater than zero")
    if resolved_cleanup == CleanupPolicy.TTL:
        if cleanup_ttl_seconds is None or cleanup_ttl_seconds <= 0:
            raise ValueError("cleanup='ttl' requires cleanup_ttl_seconds > 0")
    elif cleanup_ttl_seconds is not None:
        raise ValueError("cleanup_ttl_seconds requires cleanup='ttl'")
    if history is not None and history_store is not None:
        raise ValueError("history and history_store cannot be supplied together")
    resolved_provider_options = dict(provider_options or {})
    if command_result_config is not None:
        if not isinstance(provider, str):
            raise ValueError(
                "command_result_config must be configured on a provider instance directly"
            )
        if "command_result_config" in resolved_provider_options:
            raise ValueError(
                "command_result_config cannot also be supplied in provider_options"
            )
        resolved_provider_options["command_result_config"] = command_result_config
    resolved_provider = _resolve_provider(
        provider,
        registry=registry,
        provider_options=resolved_provider_options,
    )
    auto_sandbox_history = history is not None and history.mode == HistoryMode.SANDBOX
    auto_database_history = (
        history is not None
        and history.mode == HistoryMode.DATABASE
        and history_database_url is not None
    )
    if (
        resolved_mode == BackendMode.CREATE
        and auto_sandbox_history
        and bootstrap_outbox is None
    ):
        bootstrap_outbox = _default_bootstrap_outbox(
            resolved_provider,
            idempotency_key=idempotency_key,
        )
    resolved_history = _initial_history_store(
        history=history,
        history_store=history_store,
        mode=resolved_mode,
        ref=ref,
        provider=resolved_provider,
        transport_factory=history_transport_factory,
        database_url=history_database_url,
    )
    create_history = (
        bootstrap_outbox
        if auto_sandbox_history and bootstrap_outbox is not None
        else resolved_history
    )
    history_consistency = (
        history.consistency
        if history is not None
        else getattr(
            resolved_history,
            "config",
            HistoryConfig(mode=HistoryMode.NONE, consistency=HistoryConsistency.BEST_EFFORT),
        ).consistency
    )
    resolved_history_config = (
        history
        if history is not None
        else getattr(
            resolved_history,
            "config",
            HistoryConfig(mode=HistoryMode.NONE, consistency=HistoryConsistency.BEST_EFFORT),
        )
    )
    pipeline = OperationPipeline(
        create_history,
        retry=retry,
        consistency=history_consistency,
    )

    if resolved_mode == BackendMode.CREATE:
        if ref is not None:
            raise ValueError("ref cannot be supplied when mode='create'")
        request_metadata = dict(metadata or {})
        if sandbox_ttl_seconds is not None:
            expires_at = datetime.now(UTC) + timedelta(seconds=sandbox_ttl_seconds)
            request_metadata["agent_sandbox.expires_at"] = expires_at.isoformat().replace(
                "+00:00", "Z"
            )
        request = CreateSandboxRequest(
            image=image,
            workdir=workdir,
            env=env or {},
            metadata=request_metadata,
            idempotency_key=idempotency_key,
            sandbox_ttl_seconds=sandbox_ttl_seconds,
        )
        try:
            resolved_ref = await pipeline.run(
                "sandbox.create",
                lambda: resolved_provider.create(request),
                request={
                    "image": image,
                    "workdir": workdir,
                    "metadata": request_metadata,
                    "idempotency_key": idempotency_key,
                    "sandbox_ttl_seconds": sandbox_ttl_seconds,
                },
                result_encoder=lambda created: {
                    "provider_name": created.provider_name,
                    "provider_key": created.provider_key,
                    "sandbox_id": created.sandbox_id,
                    "sandbox_instance_id": created.sandbox_instance_id,
                },
                retryable=idempotency_key is not None,
            )
        except Exception:
            if owns_provider:
                await resolved_provider.close()
            raise
        try:
            if auto_sandbox_history:
                sandbox_history = _sandbox_history_store(
                    resolved_provider,
                    resolved_ref,
                    history or HistoryConfig(),
                    history_transport_factory,
                )
                await sandbox_history.initialize()
                if bootstrap_outbox is not None:
                    await sandbox_history.import_outbox(bootstrap_outbox)
                resolved_history = sandbox_history
            elif auto_database_history:
                database_history = _database_history_store(
                    history_database_url or "",
                    resolved_ref,
                    history or HistoryConfig(),
                )
                await database_history.initialize()
                if isinstance(create_history, MemoryHistoryStore):
                    for event in await create_history.events():
                        await database_history.append(event)
                resolved_history = database_history
        except Exception:
            if resolved_cleanup == CleanupPolicy.ON_CLOSE:
                try:
                    await resolved_provider.delete(resolved_ref)
                except Exception:
                    pass
            if owns_provider:
                await resolved_provider.close()
            raise
    else:
        if ref is None:
            raise ValueError("ref is required when mode='connect'")
        await pipeline.run(
            "sandbox.connect",
            lambda: resolved_provider.get(ref),
            sandbox_ref=ref,
            result_encoder=lambda info: {"state": info.state.value},
        )
        resolved_ref = ref

    backend = SandboxBackend(
        provider=resolved_provider,
        ref=resolved_ref,
        history_store=resolved_history,
        cleanup=resolved_cleanup,
        concurrency=concurrency,
        retry=retry,
        history_consistency=history_consistency,
        history_config=resolved_history_config,
        cleanup_ttl_seconds=cleanup_ttl_seconds,
        owns_provider=owns_provider,
        owns_history_store=owns_history_store,
        close_coordinator=close_coordinator,
    )
    try:
        for upload in uploads:
            await backend.upload_local(upload, config=upload_config)
    except Exception:
        if resolved_mode == BackendMode.CREATE and resolved_cleanup == CleanupPolicy.ON_CLOSE:
            await backend.close()
        raise
    return backend


def create_allocator(
    *,
    provider: str | SandboxProvider,
    mode: SandboxSharingMode | str = SandboxSharingMode.SHARED,
    cleanup: CleanupPolicy | str = CleanupPolicy.ON_CLOSE,
    registry: ProviderRegistry | None = None,
    provider_options: dict[str, Any] | None = None,
    **backend_options: Any,
) -> SandboxAllocator:
    owns_provider = isinstance(provider, str)
    resolved_provider = _resolve_provider(
        provider,
        registry=registry,
        provider_options=provider_options or {},
    )
    forbidden = {
        "cleanup",
        "close_coordinator",
        "metadata",
        "mode",
        "provider",
        "ref",
    }
    conflicts = forbidden.intersection(backend_options)
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise ValueError(f"Allocator backend options cannot override: {names}")
    return SandboxAllocator(
        provider=resolved_provider,
        backend_factory=create_backend,
        mode=SandboxSharingMode(mode),
        cleanup=CleanupPolicy(cleanup),
        owns_provider=owns_provider,
        backend_options=backend_options,
    )


def _initial_history_store(
    *,
    history: HistoryConfig | None,
    history_store: HistoryStore | None,
    mode: BackendMode,
    ref: SandboxRef | None,
    provider: SandboxProvider,
    transport_factory: HistoryTransportFactory | None,
    database_url: str | None,
) -> HistoryStore:
    if history_store is not None:
        return history_store
    if history is None:
        return NoneHistoryStore()
    if history.mode in {HistoryMode.NONE, HistoryMode.PROVIDER}:
        return NoneHistoryStore()
    if history.mode == HistoryMode.DATABASE:
        if database_url is None:
            raise ValueError(
                "database history mode requires history_database_url or history_store"
            )
        if mode == BackendMode.CONNECT:
            if ref is None:
                raise ValueError("ref is required for database history connect mode")
            return _database_history_store(database_url, ref, history)
        return MemoryHistoryStore()
    if mode == BackendMode.CONNECT:
        if ref is None:
            raise ValueError("ref is required for sandbox history connect mode")
        return _sandbox_history_store(provider, ref, history, transport_factory)
    return MemoryHistoryStore()


def _sandbox_history_store(
    provider: SandboxProvider,
    ref: SandboxRef,
    config: HistoryConfig,
    transport_factory: HistoryTransportFactory | None,
) -> SandboxHistoryStore:
    transport = (
        transport_factory(provider, ref)
        if transport_factory is not None
        else ProviderHistoryHelperTransport(provider, ref)
    )
    return SandboxHistoryStore(transport, sdk_version=SDK_VERSION, config=config)


def _database_history_store(
    url: str,
    ref: SandboxRef,
    config: HistoryConfig,
) -> SQLAlchemyHistoryStore:
    from agent_sandbox_backends.history.sqlalchemy import SQLAlchemyHistoryStore

    return SQLAlchemyHistoryStore(url, identity=ref, config=config)


def _resolve_provider(
    provider: str | SandboxProvider,
    *,
    registry: ProviderRegistry | None,
    provider_options: dict[str, Any],
) -> SandboxProvider:
    if not isinstance(provider, str):
        if provider_options:
            raise ValueError("provider_options cannot be used with a provider instance")
        return provider
    resolved_registry = registry or ProviderRegistry.with_builtins()
    return resolved_registry.create(provider, **provider_options)


def _parse_opensandbox_url(url: str) -> tuple[str, str]:
    value = url.strip()
    if not value:
        raise ValueError("url must not be empty")
    if "://" not in value:
        value = f"http://{value}"
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("OpenSandbox URL scheme must be http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("OpenSandbox URL must not contain credentials")
    if not parsed.hostname or not parsed.netloc:
        raise ValueError("OpenSandbox URL must contain a hostname")
    try:
        _ = parsed.port
    except ValueError as error:
        raise ValueError("OpenSandbox URL contains an invalid port") from error
    if parsed.path not in {"", "/"}:
        raise ValueError("OpenSandbox URL must be the service root without /v1 or other paths")
    if parsed.query or parsed.fragment:
        raise ValueError("OpenSandbox URL must not contain a query string or fragment")
    return parsed.scheme, parsed.netloc


def _metadata_with_sandbox_name(
    metadata: dict[str, str] | None,
    *,
    sandbox_name: str | None,
    mode: BackendMode | str,
) -> dict[str, str] | None:
    if sandbox_name is None:
        return metadata
    if BackendMode(mode) != BackendMode.CREATE:
        raise ValueError("sandbox_name can only be supplied when mode='create'")
    resolved_name = sandbox_name.strip()
    if not resolved_name:
        raise ValueError("sandbox_name must not be empty")
    if len(resolved_name) > 128:
        raise ValueError("sandbox_name must not exceed 128 characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in resolved_name):
        raise ValueError("sandbox_name must not contain control characters")
    resolved_metadata = dict(metadata or {})
    existing = resolved_metadata.get(SANDBOX_NAME_METADATA_KEY)
    if existing is not None and existing != resolved_name:
        raise ValueError(
            f"metadata[{SANDBOX_NAME_METADATA_KEY!r}] conflicts with sandbox_name"
        )
    resolved_metadata[SANDBOX_NAME_METADATA_KEY] = resolved_name
    return resolved_metadata


def _default_bootstrap_outbox(
    provider: SandboxProvider,
    *,
    idempotency_key: str | None,
) -> BootstrapOutbox:
    identity = idempotency_key or str(uuid7())
    digest = hashlib.sha256(
        f"{provider.provider_name}:{provider.provider_key}:{identity}".encode()
    ).hexdigest()
    root = Path(tempfile.gettempdir()) / "agent-sandbox-backends" / "bootstrap-outbox"
    return BootstrapOutbox(root / digest)
