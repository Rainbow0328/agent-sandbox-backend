from __future__ import annotations

from typing import Any, Protocol


class LeaseStore(Protocol):
    async def lease_acquire(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]: ...

    async def lease_renew(
        self,
        resource_key: str,
        owner_id: str,
        ttl_ms: int,
    ) -> dict[str, Any]: ...

    async def lease_release(self, resource_key: str, owner_id: str) -> dict[str, Any]: ...
