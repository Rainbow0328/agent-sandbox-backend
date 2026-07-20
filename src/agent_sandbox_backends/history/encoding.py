from __future__ import annotations

import base64
from typing import Any

from agent_sandbox_backends.domain.commands import CommandOutputChunk
from agent_sandbox_backends.domain.operations import OperationEvent, OperationStatus
from agent_sandbox_backends.helper.source import history_helper
from agent_sandbox_backends.history.config import HistoryConfig


class HistoryEventEncoder:
    def __init__(self, config: HistoryConfig) -> None:
        self.config = config

    def envelope(self, event: OperationEvent) -> dict[str, Any]:
        records: list[dict[str, Any]]
        if event.status == OperationStatus.STARTED:
            records = [
                {
                    "type": "operation_started",
                    "change_id": f"{event.event_id}:created",
                    "event": self._started_payload(event),
                }
            ]
        else:
            records = []
            output_record, output_complete, storage_state = self._command_output_record(event)
            if output_record is not None:
                records.append(output_record)
            records.append(
                {
                    "type": "operation_completed",
                    "change_id": f"{event.event_id}:completed:{event.status.value}",
                    "event": self._completed_payload(
                        event,
                        output_complete=output_complete,
                        storage_state=storage_state,
                    ),
                }
            )
        return {
            "batch_id": f"{event.event_id}:{event.status.value}",
            "schema_version": history_helper.SCHEMA_VERSION,
            "records": records,
        }

    def output_envelope(
        self,
        event_id: str,
        chunks: tuple[CommandOutputChunk, ...],
    ) -> dict[str, Any]:
        encoded_chunks = [
            {
                "stream": chunk.stream.value,
                "chunk_index": chunk.chunk_index,
                "data_base64": base64.b64encode(chunk.data).decode("ascii"),
                "encoding": "identity",
                "original_size": len(chunk.data),
            }
            for chunk in chunks
        ]
        indexes = ",".join(
            f"{chunk.stream.value}:{chunk.chunk_index}" for chunk in chunks
        )
        return {
            "batch_id": f"{event_id}:output:{indexes}",
            "schema_version": history_helper.SCHEMA_VERSION,
            "records": [
                {
                    "type": "command_output",
                    "change_id": f"{event_id}:output:{indexes}",
                    "event_id": event_id,
                    "chunks": encoded_chunks,
                }
            ],
        }

    @staticmethod
    def _started_payload(event: OperationEvent) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": event.event_id,
            "operation_type": event.operation_type,
            "occurred_at": event.occurred_at.isoformat().replace("+00:00", "Z"),
            "request": event.request,
            "actor": event.actor.model_dump(mode="json"),
            "schema_version": event.schema_version,
        }
        if event.operation_type == "command.execute":
            timeout_seconds = event.request.get("timeout_seconds")
            payload["command"] = {
                "command": str(event.request.get("command", "")),
                "cwd": str(event.request.get("cwd") or "/workspace"),
                "timeout_ms": (
                    int(float(timeout_seconds) * 1000) if timeout_seconds is not None else None
                ),
            }
        elif event.operation_type.startswith("file."):
            payload["file_operation"] = {
                "path": str(event.request.get("path", "")),
                "change_type": event.operation_type.removeprefix("file."),
            }
        return payload

    @staticmethod
    def _completed_payload(
        event: OperationEvent,
        *,
        output_complete: bool,
        storage_state: str,
    ) -> dict[str, Any]:
        result = dict(event.result or {})
        result.pop("stdout_base64", None)
        result.pop("stderr_base64", None)
        payload: dict[str, Any] = {
            "event_id": event.event_id,
            "status": event.status.value,
            "completed_at": event.occurred_at.isoformat().replace("+00:00", "Z"),
            "duration_ms": event.duration_ms,
            "result": result or None,
        }
        if event.operation_type == "command.execute" and event.result is not None:
            payload["command"] = {
                "exit_code": event.result.get("exit_code"),
                "provider_command_id": event.result.get("command_id"),
                "output_complete": output_complete,
                "history_storage_state": storage_state,
            }
        return payload

    def _command_output_record(
        self,
        event: OperationEvent,
    ) -> tuple[dict[str, Any] | None, bool, str]:
        if event.operation_type != "command.execute" or event.result is None:
            return None, True, "complete"

        declared_complete = bool(event.result.get("output_complete", True))
        declared_state = str(event.result.get("history_storage_state", "complete"))

        streams = (
            ("stdout", "stdout_base64", self.config.capture_stdout),
            ("stderr", "stderr_base64", self.config.capture_stderr),
        )
        remaining = self.config.max_operation_output_bytes
        chunks: list[dict[str, Any]] = []
        output_complete = declared_complete
        for stream, result_key, capture in streams:
            if not capture:
                continue
            encoded = event.result.get(result_key)
            if not isinstance(encoded, str) or not encoded:
                continue
            data = base64.b64decode(encoded, validate=True)
            captured = data[:remaining]
            if len(captured) < len(data):
                output_complete = False
            remaining -= len(captured)
            for chunk_index, offset in enumerate(
                range(0, len(captured), self.config.output_chunk_bytes)
            ):
                chunk = captured[offset : offset + self.config.output_chunk_bytes]
                chunks.append(
                    {
                        "stream": stream,
                        "chunk_index": chunk_index,
                        "data_base64": base64.b64encode(chunk).decode("ascii"),
                        "encoding": "identity",
                        "original_size": len(chunk),
                        "created_at": event.occurred_at.isoformat().replace("+00:00", "Z"),
                    }
                )
            if remaining == 0:
                output_complete = output_complete and len(captured) == len(data)

        if not chunks:
            return None, output_complete, declared_state
        return (
            {
                "type": "command_output",
                "change_id": f"{event.event_id}:output:0-{len(chunks) - 1}",
                "event_id": event.event_id,
                "chunks": chunks,
            },
            output_complete,
            "complete" if output_complete else "truncated_by_policy",
        )
