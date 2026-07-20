from __future__ import annotations

from collections.abc import Callable
from typing import Any

from agent_sandbox_backends.domain.errors import ProviderError
from agent_sandbox_backends.ports.provider import SandboxProvider
from agent_sandbox_backends.providers.mock import MockSandboxProvider

ProviderFactory = Callable[..., SandboxProvider]


class ProviderRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, ProviderFactory] = {}

    @classmethod
    def with_builtins(cls) -> ProviderRegistry:
        registry = cls()
        registry.register("mock", MockSandboxProvider)
        try:
            from agent_sandbox_backends.providers.opensandbox import OpenSandboxProvider
        except ImportError:
            pass
        else:
            registry.register("opensandbox", OpenSandboxProvider)
        return registry

    def register(
        self,
        name: str,
        factory: ProviderFactory,
        *,
        override: bool = False,
    ) -> None:
        if name in self._factories and not override:
            raise ValueError(f"Provider is already registered: {name}")
        self._factories[name] = factory

    def create(self, name: str, **options: Any) -> SandboxProvider:
        try:
            factory = self._factories[name]
        except KeyError as error:
            raise ProviderError(
                f"Provider is not registered: {name}",
                provider_name=name,
                operation="provider.resolve",
            ) from error
        return factory(**options)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))
