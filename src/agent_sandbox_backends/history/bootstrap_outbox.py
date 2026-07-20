from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.operations import OperationEvent


class BootstrapOutbox:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self._lock = asyncio.Lock()

    async def put(self, event: OperationEvent) -> Path:
        async with self._lock:
            return await asyncio.to_thread(self._put_sync, event)

    async def append(self, event: OperationEvent) -> None:
        await self.put(event)

    async def events(self) -> tuple[OperationEvent, ...]:
        async with self._lock:
            return await asyncio.to_thread(self._events_sync)

    async def remove(self, event_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._remove_sync, event_id)

    async def mark_imported(self, event: OperationEvent) -> None:
        async with self._lock:
            await asyncio.to_thread(self._mark_imported_sync, event)

    async def close(self) -> None:
        return None

    async def append_output(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> None:
        del event_id, chunks

    def _put_sync(self, event: OperationEvent) -> Path:
        self.directory.mkdir(parents=True, exist_ok=True)
        target = self._event_path(event.event_id, event.status.value)
        event_payload = event.model_dump(mode="json")
        existing = self._read_record(target) if target.exists() else None
        metadata = self._metadata_for_event(event, event_payload)
        if existing is not None:
            previous = self._record_metadata(existing)
            metadata["created_at"] = previous.get("created_at", metadata["created_at"])
            metadata["imported_at"] = previous.get("imported_at")
        payload = self._encode_record(event_payload, metadata)
        self._write_record(target, payload)
        self._merge_event_metadata(event.event_id)
        return target

    def _events_sync(self) -> tuple[OperationEvent, ...]:
        if not self.directory.exists():
            return ()
        events: list[OperationEvent] = []
        for path in sorted(self.directory.glob("*.json")):
            record = self._read_record(path)
            payload = record.get("event", record)
            if not isinstance(payload, dict):
                raise ValueError(f"Invalid outbox event payload: {path}")
            events.append(
                OperationEvent.model_validate_json(
                    self._canonical_json(cast(dict[str, Any], payload))
                )
            )
        return tuple(events)

    def _mark_imported_sync(self, event: OperationEvent) -> None:
        path = self._event_path(event.event_id, event.status.value)
        if not path.exists():
            return
        record = self._read_record(path)
        metadata = self._record_metadata(record)
        metadata["imported_at"] = self._now()
        payload = self._encode_record(self._event_payload(record), metadata)
        self._write_record(path, payload)

    def _remove_sync(self, event_id: str) -> None:
        self._validate_event_id(event_id)
        for path in self.directory.glob(f"{event_id}.*.json"):
            path.unlink(missing_ok=True)

    def _event_path(self, event_id: str, status: str) -> Path:
        self._validate_event_id(event_id)
        if not status.replace("_", "").isalnum():
            raise ValueError("status contains unsafe path characters")
        return self.directory / f"{event_id}.{status}.json"

    def _merge_event_metadata(self, event_id: str) -> None:
        records = [
            (path, self._read_record(path))
            for path in sorted(self.directory.glob(f"{event_id}.*.json"))
        ]
        merged: dict[str, Any] = {}
        for _, record in records:
            metadata = self._record_metadata(record)
            for key in (
                "idempotency_key",
                "provider_name",
                "provider_key",
                "sandbox_id",
                "sandbox_instance_id",
            ):
                if metadata.get(key) is not None:
                    merged[key] = metadata[key]
        for path, record in records:
            metadata = self._record_metadata(record)
            changed = False
            for key, value in merged.items():
                if metadata.get(key) is None:
                    metadata[key] = value
                    changed = True
            if changed:
                self._write_record(
                    path,
                    self._encode_record(self._event_payload(record), metadata),
                )

    @classmethod
    def _metadata_for_event(
        cls,
        event: OperationEvent,
        event_payload: dict[str, Any],
    ) -> dict[str, Any]:
        result = event.result or {}
        ref = event.sandbox_ref
        payload = cls._canonical_json(event_payload)
        return {
            "event_id": event.event_id,
            "idempotency_key": event.request.get("idempotency_key"),
            "provider_name": ref.provider_name if ref is not None else result.get("provider_name"),
            "provider_key": ref.provider_key if ref is not None else result.get("provider_key"),
            "sandbox_id": ref.sandbox_id if ref is not None else result.get("sandbox_id"),
            "sandbox_instance_id": (
                ref.sandbox_instance_id
                if ref is not None
                else result.get("sandbox_instance_id")
            ),
            "payload_hash": hashlib.sha256(payload).hexdigest(),
            "created_at": cls._now(),
            "imported_at": None,
        }

    @staticmethod
    def _read_record(path: Path) -> dict[str, Any]:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"Invalid outbox record: {path}")
        return cast(dict[str, Any], value)

    @staticmethod
    def _event_payload(record: dict[str, Any]) -> dict[str, Any]:
        payload = record.get("event", record)
        if not isinstance(payload, dict):
            raise ValueError("Invalid outbox event payload")
        return cast(dict[str, Any], payload)

    @staticmethod
    def _record_metadata(record: dict[str, Any]) -> dict[str, Any]:
        metadata = record.get("metadata")
        if isinstance(metadata, dict):
            typed = cast(dict[str, Any], metadata)
            return dict(typed)
        return {}

    @staticmethod
    def _write_record(path: Path, payload: bytes) -> None:
        temporary = path.with_suffix(".json.tmp")
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    @classmethod
    def _encode_record(
        cls,
        event_payload: dict[str, Any],
        metadata: dict[str, Any],
    ) -> bytes:
        return cls._canonical_json(
            {"version": 1, "event": event_payload, "metadata": metadata}
        )

    @staticmethod
    def _canonical_json(value: dict[str, Any]) -> bytes:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _validate_event_id(event_id: str) -> None:
        allowed = "0123456789abcdefghijklmnopqrstuvwxyz-"
        if not event_id or any(character not in allowed for character in event_id.lower()):
            raise ValueError("event_id contains unsafe path characters")
