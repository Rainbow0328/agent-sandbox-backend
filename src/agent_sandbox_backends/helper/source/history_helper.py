from __future__ import annotations

import argparse
import base64
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

SCHEMA_VERSION = 1
DEFAULT_DATABASE_PATH = "/.agent-history/history.sqlite3"

SCHEMA_V1_SQL = """
CREATE TABLE IF NOT EXISTS history_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS history_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    provider_key TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    sandbox_instance_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'sdk',
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    thread_id TEXT,
    run_id TEXT,
    correlation_id TEXT,
    operation_type TEXT NOT NULL,
    status TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    completed_at TEXT,
    duration_ms INTEGER,
    request_json TEXT,
    result_json TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_history_sandbox_seq
ON history_events(provider_key, sandbox_id, sandbox_instance_id, seq);

CREATE INDEX IF NOT EXISTS idx_history_run
ON history_events(thread_id, run_id, seq);

CREATE INDEX IF NOT EXISTS idx_history_operation
ON history_events(operation_type, status, seq);

CREATE TABLE IF NOT EXISTS history_changes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    change_id TEXT NOT NULL UNIQUE,
    event_id TEXT NOT NULL,
    change_type TEXT NOT NULL
        CHECK(change_type IN ('created', 'output_appended', 'completed', 'storage_state_changed')),
    changed_at TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_history_changes_event
ON history_changes(event_id, seq);

CREATE TABLE IF NOT EXISTS command_executions (
    event_id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    environment_json TEXT,
    timeout_ms INTEGER,
    exit_code INTEGER,
    provider_command_id TEXT,
    output_complete INTEGER NOT NULL DEFAULT 1,
    history_storage_state TEXT NOT NULL DEFAULT 'complete',
    FOREIGN KEY(event_id) REFERENCES history_events(event_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS command_output_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    stream TEXT NOT NULL CHECK(stream IN ('stdout', 'stderr')),
    chunk_index INTEGER NOT NULL,
    data BLOB NOT NULL,
    encoding TEXT NOT NULL DEFAULT 'identity'
        CHECK(encoding IN ('identity', 'gzip', 'zlib')),
    original_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, stream, chunk_index),
    FOREIGN KEY(event_id) REFERENCES history_events(event_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_command_output_chunks
ON command_output_chunks(event_id, stream, chunk_index);

CREATE TABLE IF NOT EXISTS file_operations (
    event_id TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    old_path TEXT,
    change_type TEXT NOT NULL,
    before_hash TEXT,
    after_hash TEXT,
    before_size INTEGER,
    after_size INTEGER,
    diff_data BLOB,
    diff_encoding TEXT
        CHECK(diff_encoding IS NULL OR diff_encoding IN ('identity', 'gzip', 'zlib')),
    diff_complete INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY(event_id) REFERENCES history_events(event_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS history_consumers (
    consumer_id TEXT PRIMARY KEY,
    acknowledged_seq INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS history_leases (
    resource_key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class HelperError(RuntimeError):
    pass


class ConfigConflictError(HelperError):
    pass


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def connect(
    database_path: str | os.PathLike[str],
    busy_timeout_ms: int = 5000,
) -> sqlite3.Connection:
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=busy_timeout_ms / 1000)
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
    deadline = time.monotonic() + busy_timeout_ms / 1000
    try:
        while True:
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as error:
                if "locked" not in str(error).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA wal_autocheckpoint=1000")
        return connection
    except Exception:
        connection.close()
        raise


def initialize(
    database_path: str | os.PathLike[str],
    identity: dict[str, Any],
    config: dict[str, Any] | None = None,
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    required = {
        "provider_key",
        "sandbox_id",
        "sandbox_instance_id",
        "sdk_version",
        "schema_version",
    }
    missing = sorted(required - identity.keys())
    if missing:
        raise HelperError(f"Identity is missing required fields: {', '.join(missing)}")
    if int(identity["schema_version"]) != SCHEMA_VERSION:
        raise HelperError(f"Unsupported schema version: {identity['schema_version']}")

    connection = connect(database_path, busy_timeout_ms)
    try:
        current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if current_version > SCHEMA_VERSION:
            raise HelperError(f"Database schema {current_version} is newer than helper")
        with connection:
            connection.executescript(SCHEMA_V1_SQL)
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            existing = _meta_dict(connection)
            for key in ("provider_key", "sandbox_id", "sandbox_instance_id"):
                if key in existing and existing[key] != str(identity[key]):
                    raise HelperError(f"History identity conflict for {key}")

            created_at = existing.get("created_at", utc_now())
            config_json = canonical_json(config or {})
            defaults = {
                "schema_version": str(SCHEMA_VERSION),
                "provider_key": str(identity["provider_key"]),
                "sandbox_id": str(identity["sandbox_id"]),
                "sandbox_instance_id": str(identity["sandbox_instance_id"]),
                "created_at": created_at,
                "sdk_version": str(identity["sdk_version"]),
                "history_config_version": existing.get("history_config_version", "1"),
                "history_config_revision": existing.get("history_config_revision", "0"),
                "history_config_json": existing.get("history_config_json", config_json),
                "history_config_updated_at": existing.get("history_config_updated_at", created_at),
                "history_config_updated_by": existing.get(
                    "history_config_updated_by",
                    "sdk:initialize",
                ),
            }
            sandbox_name = str(identity.get("sandbox_name") or "").strip()
            if sandbox_name:
                defaults["sandbox_name"] = sandbox_name
            sandbox_metadata = identity.get("sandbox_metadata")
            if sandbox_metadata is not None:
                defaults["sandbox_metadata_json"] = canonical_json(
                    _object(sandbox_metadata, "sandbox_metadata")
                )
            connection.executemany(
                "INSERT OR REPLACE INTO history_meta(key, value) VALUES (?, ?)",
                defaults.items(),
            )
        return health(database_path, busy_timeout_ms=busy_timeout_ms)
    finally:
        connection.close()


def health(
    database_path: str | os.PathLike[str],
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    connection = connect(database_path, busy_timeout_ms)
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        meta = _meta_dict(connection)
        return {
            "schema_version": version,
            "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]),
            "identity": {
                "provider_key": meta.get("provider_key"),
                "sandbox_id": meta.get("sandbox_id"),
                "sandbox_instance_id": meta.get("sandbox_instance_id"),
                "sandbox_name": meta.get("sandbox_name"),
                "sandbox_metadata": _decode_json_object(
                    meta.get("sandbox_metadata_json")
                ),
            },
        }
    finally:
        connection.close()


def apply_batch(
    database_path: str | os.PathLike[str],
    envelope: dict[str, Any],
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if int(envelope.get("schema_version", 0)) != SCHEMA_VERSION:
        raise HelperError("Apply envelope schema_version must be 1")
    batch_id = str(envelope.get("batch_id") or "")
    records = _object_list(envelope.get("records"), "records")
    if not batch_id or not records:
        raise HelperError("Apply envelope requires batch_id and non-empty records")

    connection = connect(database_path, busy_timeout_ms)
    applied = 0
    try:
        with connection:
            for index, record in enumerate(records):
                record_type = record.get("type")
                change_id = str(record.get("change_id") or f"{batch_id}:{index}:{record_type}")
                if record_type == "operation_started":
                    _apply_operation_started(connection, record, change_id)
                elif record_type == "command_output":
                    _apply_command_output(connection, record, change_id)
                elif record_type == "operation_completed":
                    _apply_operation_completed(connection, record, change_id)
                elif record_type == "storage_state_changed":
                    _apply_storage_state(connection, record, change_id)
                else:
                    raise HelperError(f"Unsupported record type: {record_type}")
                applied += 1
        return {"batch_id": batch_id, "records_applied": applied}
    finally:
        connection.close()


def _apply_operation_started(
    connection: sqlite3.Connection,
    record: dict[str, Any],
    change_id: str,
) -> None:
    event = _required_dict(record, "event")
    event_id = _required_text(event, "event_id")
    identity = _identity(connection)
    actor = _optional_object(event.get("actor")) or {}
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO history_events(
            event_id, provider_key, sandbox_id, sandbox_instance_id, source,
            actor_type, actor_id, thread_id, run_id, correlation_id,
            operation_type, status, occurred_at, request_json, schema_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?, ?, ?)
        """,
        (
            event_id,
            identity["provider_key"],
            identity["sandbox_id"],
            identity["sandbox_instance_id"],
            str(event.get("source", "sdk")),
            str(actor.get("actor_type", "system")),
            actor.get("actor_id"),
            actor.get("thread_id"),
            actor.get("run_id"),
            actor.get("correlation_id"),
            _required_text(event, "operation_type"),
            _required_text(event, "occurred_at"),
            _optional_json(event.get("request")),
            int(event.get("schema_version", SCHEMA_VERSION)),
        ),
    )
    if cursor.rowcount == 0:
        existing = connection.execute(
            "SELECT operation_type FROM history_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if existing is None or existing["operation_type"] != event["operation_type"]:
            raise HelperError(f"Conflicting duplicate operation start: {event_id}")

    command = _optional_object(event.get("command"))
    if command is not None:
        connection.execute(
            """
            INSERT OR IGNORE INTO command_executions(
                event_id, command, cwd, environment_json, timeout_ms
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event_id,
                _required_text(command, "command"),
                str(command.get("cwd", "/workspace")),
                _optional_json(command.get("environment")),
                command.get("timeout_ms"),
            ),
        )

    file_operation = _optional_object(event.get("file_operation"))
    if file_operation is not None:
        connection.execute(
            """
            INSERT OR IGNORE INTO file_operations(
                event_id, path, old_path, change_type, before_hash, after_hash,
                before_size, after_size, diff_complete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                _required_text(file_operation, "path"),
                file_operation.get("old_path"),
                _required_text(file_operation, "change_type"),
                file_operation.get("before_hash"),
                file_operation.get("after_hash"),
                file_operation.get("before_size"),
                file_operation.get("after_size"),
                int(bool(file_operation.get("diff_complete", True))),
            ),
        )
    _append_change(connection, change_id, event_id, "created", event["occurred_at"])


def _apply_command_output(
    connection: sqlite3.Connection,
    record: dict[str, Any],
    change_id: str,
) -> None:
    event_id = _required_text(record, "event_id")
    chunks = _object_list(record.get("chunks"), "chunks")
    if not chunks:
        raise HelperError("command_output requires non-empty chunks")
    _require_event(connection, event_id)
    for chunk in chunks:
        data = base64.b64decode(_required_text(chunk, "data_base64"), validate=True)
        connection.execute(
            """
            INSERT OR IGNORE INTO command_output_chunks(
                event_id, stream, chunk_index, data, encoding, original_size, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                _required_text(chunk, "stream"),
                int(chunk["chunk_index"]),
                data,
                str(chunk.get("encoding", "identity")),
                int(chunk.get("original_size", len(data))),
                str(chunk.get("created_at", utc_now())),
            ),
        )
    _append_change(connection, change_id, event_id, "output_appended", utc_now())


def _apply_operation_completed(
    connection: sqlite3.Connection,
    record: dict[str, Any],
    change_id: str,
) -> None:
    event = _required_dict(record, "event")
    event_id = _required_text(event, "event_id")
    row = _require_event(connection, event_id)
    status = _required_text(event, "status")
    if status == "started":
        raise HelperError("operation_completed requires a terminal status")
    if row["status"] != "started" and row["status"] != status:
        raise HelperError(f"Conflicting terminal status for event: {event_id}")
    connection.execute(
        """
        UPDATE history_events
        SET status = ?, completed_at = ?, duration_ms = ?, result_json = ?
        WHERE event_id = ?
        """,
        (
            status,
            _required_text(event, "completed_at"),
            event.get("duration_ms"),
            _optional_json(event.get("result")),
            event_id,
        ),
    )
    command = _optional_object(event.get("command"))
    if command is not None:
        connection.execute(
            """
            UPDATE command_executions
            SET exit_code = ?, provider_command_id = ?, output_complete = ?,
                history_storage_state = ?
            WHERE event_id = ?
            """,
            (
                command.get("exit_code"),
                command.get("provider_command_id"),
                int(bool(command.get("output_complete", True))),
                str(command.get("history_storage_state", "complete")),
                event_id,
            ),
        )
    _append_change(connection, change_id, event_id, "completed", event["completed_at"])


def _apply_storage_state(
    connection: sqlite3.Connection,
    record: dict[str, Any],
    change_id: str,
) -> None:
    event_id = _required_text(record, "event_id")
    _require_event(connection, event_id)
    connection.execute(
        "UPDATE command_executions SET history_storage_state = ? WHERE event_id = ?",
        (_required_text(record, "history_storage_state"), event_id),
    )
    _append_change(connection, change_id, event_id, "storage_state_changed", utc_now())


def query_changes(
    database_path: str | os.PathLike[str],
    *,
    after_change_seq: int,
    limit: int,
    max_bytes: int,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if after_change_seq < 0 or limit < 1 or max_bytes < 1:
        raise HelperError("Invalid query bounds")
    connection = connect(database_path, busy_timeout_ms)
    try:
        bounds = connection.execute(
            "SELECT MIN(seq) AS min_seq, MAX(seq) AS max_seq FROM history_changes"
        ).fetchone()
        min_seq = int(bounds["min_seq"] or 0)
        max_seq = int(bounds["max_seq"] or 0)
        reset_required = min_seq > 0 and after_change_seq < min_seq - 1
        rows = connection.execute(
            """
            SELECT c.seq AS source_seq, c.change_id, c.event_id, c.change_type,
                   c.changed_at, c.schema_version, e.operation_type, e.status
            FROM history_changes AS c
            JOIN history_events AS e ON e.event_id = c.event_id
            WHERE c.seq > ?
            ORDER BY c.seq
            LIMIT ?
            """,
            (after_change_seq, limit),
        ).fetchall()
        changes: list[dict[str, Any]] = []
        used_bytes = 0
        for row in rows:
            item = dict(row)
            item_bytes = len(canonical_json(item).encode("utf-8"))
            if changes and used_bytes + item_bytes > max_bytes:
                break
            if item_bytes > max_bytes:
                raise HelperError("Single change exceeds query max_bytes")
            changes.append(item)
            used_bytes += item_bytes
        next_seq = changes[-1]["source_seq"] if changes else after_change_seq
        return {
            "changes": changes,
            "next_change_seq": next_seq,
            "min_available_change_seq": min_seq,
            "max_change_seq": max_seq,
            "reset_required": reset_required,
        }
    finally:
        connection.close()


def get_operation(
    database_path: str | os.PathLike[str],
    event_id: str,
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    connection = connect(database_path, busy_timeout_ms)
    try:
        event = connection.execute(
            "SELECT * FROM history_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if event is None:
            raise HelperError(f"Operation does not exist: {event_id}")
        result = dict(event)
        result["request"] = _decode_json(result.pop("request_json"))
        result["result"] = _decode_json(result.pop("result_json"))
        command = connection.execute(
            "SELECT * FROM command_executions WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        file_operation = connection.execute(
            "SELECT * FROM file_operations WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        result["command"] = dict(command) if command is not None else None
        result["file_operation"] = dict(file_operation) if file_operation is not None else None
        return result
    finally:
        connection.close()


def get_output(
    database_path: str | os.PathLike[str],
    event_id: str,
    stream: str,
    *,
    after_chunk: int = -1,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if stream not in {"stdout", "stderr"}:
        raise HelperError("stream must be stdout or stderr")
    connection = connect(database_path, busy_timeout_ms)
    try:
        _require_event(connection, event_id)
        rows = connection.execute(
            """
            SELECT chunk_index, data, encoding, original_size, created_at
            FROM command_output_chunks
            WHERE event_id = ? AND stream = ? AND chunk_index > ?
            ORDER BY chunk_index
            """,
            (event_id, stream, after_chunk),
        ).fetchall()
        return {
            "event_id": event_id,
            "stream": stream,
            "chunks": [
                {
                    "chunk_index": row["chunk_index"],
                    "data_base64": base64.b64encode(row["data"]).decode("ascii"),
                    "encoding": row["encoding"],
                    "original_size": row["original_size"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }
    finally:
        connection.close()


def acknowledge(
    database_path: str | os.PathLike[str],
    consumer_id: str,
    seq: int,
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if not consumer_id or seq < 0:
        raise HelperError("Invalid consumer acknowledgement")
    connection = connect(database_path, busy_timeout_ms)
    try:
        with connection:
            connection.execute(
                """
                INSERT INTO history_consumers(consumer_id, acknowledged_seq, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                    acknowledged_seq = MAX(
                        history_consumers.acknowledged_seq,
                        excluded.acknowledged_seq
                    ),
                    updated_at = excluded.updated_at
                """,
                (consumer_id, seq, utc_now()),
            )
        row = connection.execute(
            "SELECT acknowledged_seq, updated_at FROM history_consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        return {"consumer_id": consumer_id, **dict(row)}
    finally:
        connection.close()


def config_get(
    database_path: str | os.PathLike[str],
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    connection = connect(database_path, busy_timeout_ms)
    try:
        meta = _meta_dict(connection)
        return {
            "version": int(meta.get("history_config_version", "1")),
            "revision": int(meta.get("history_config_revision", "0")),
            "config": _decode_json(meta.get("history_config_json")) or {},
            "updated_at": meta.get("history_config_updated_at"),
            "updated_by": meta.get("history_config_updated_by"),
        }
    finally:
        connection.close()


def config_set(
    database_path: str | os.PathLike[str],
    changes: dict[str, Any],
    *,
    expected_revision: int,
    updated_by: str,
    allowed_fields: set[str] | None = None,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if not updated_by:
        raise HelperError("updated_by is required")
    if allowed_fields is not None:
        invalid = sorted(set(changes) - allowed_fields)
        if invalid:
            raise HelperError(f"Config contains unsupported fields: {', '.join(invalid)}")
    connection = connect(database_path, busy_timeout_ms)
    try:
        with connection:
            meta = _meta_dict(connection)
            current_revision = int(meta.get("history_config_revision", "0"))
            if current_revision != expected_revision:
                raise ConfigConflictError(
                    f"Expected config revision {expected_revision}, got {current_revision}"
                )
            current = _decode_json_object(meta.get("history_config_json"))
            current.update(changes)
            next_revision = current_revision + 1
            updated_at = utc_now()
            values = {
                "history_config_revision": str(next_revision),
                "history_config_json": canonical_json(current),
                "history_config_updated_at": updated_at,
                "history_config_updated_by": updated_by,
            }
            connection.executemany(
                "INSERT OR REPLACE INTO history_meta(key, value) VALUES (?, ?)",
                values.items(),
            )
        return config_get(database_path, busy_timeout_ms=busy_timeout_ms)
    finally:
        connection.close()


def lease_acquire(
    database_path: str | os.PathLike[str],
    resource_key: str,
    owner_id: str,
    ttl_ms: int,
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if not resource_key or not owner_id or ttl_ms < 1:
        raise HelperError("Invalid lease parameters")
    now = datetime.now(UTC)
    expires_at = (now + timedelta(milliseconds=ttl_ms)).isoformat().replace("+00:00", "Z")
    now_text = now.isoformat().replace("+00:00", "Z")
    connection = connect(database_path, busy_timeout_ms)
    try:
        with connection:
            existing = connection.execute(
                "SELECT owner_id, expires_at FROM history_leases WHERE resource_key = ?",
                (resource_key,),
            ).fetchone()
            if existing is not None and existing["owner_id"] != owner_id:
                if _parse_time(existing["expires_at"]) > now:
                    return {
                        "acquired": False,
                        "resource_key": resource_key,
                        "owner_id": existing["owner_id"],
                        "expires_at": existing["expires_at"],
                    }
            connection.execute(
                """
                INSERT INTO history_leases(resource_key, owner_id, expires_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(resource_key) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (resource_key, owner_id, expires_at, now_text),
            )
        return {
            "acquired": True,
            "resource_key": resource_key,
            "owner_id": owner_id,
            "expires_at": expires_at,
        }
    finally:
        connection.close()


def lease_renew(
    database_path: str | os.PathLike[str],
    resource_key: str,
    owner_id: str,
    ttl_ms: int,
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    expires_at = (now + timedelta(milliseconds=ttl_ms)).isoformat().replace("+00:00", "Z")
    connection = connect(database_path, busy_timeout_ms)
    try:
        with connection:
            cursor = connection.execute(
                """
                UPDATE history_leases
                SET expires_at = ?, updated_at = ?
                WHERE resource_key = ? AND owner_id = ? AND expires_at > ?
                """,
                (expires_at, utc_now(), resource_key, owner_id, utc_now()),
            )
            if cursor.rowcount != 1:
                return {"renewed": False, "resource_key": resource_key, "owner_id": owner_id}
        return {
            "renewed": True,
            "resource_key": resource_key,
            "owner_id": owner_id,
            "expires_at": expires_at,
        }
    finally:
        connection.close()


def lease_release(
    database_path: str | os.PathLike[str],
    resource_key: str,
    owner_id: str,
    *,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    connection = connect(database_path, busy_timeout_ms)
    try:
        with connection:
            cursor = connection.execute(
                "DELETE FROM history_leases WHERE resource_key = ? AND owner_id = ?",
                (resource_key, owner_id),
            )
        return {"released": cursor.rowcount == 1, "resource_key": resource_key}
    finally:
        connection.close()


def cleanup(
    database_path: str | os.PathLike[str],
    *,
    ttl_days: int | None,
    max_bytes: int,
    overflow_policy: str,
    if_due: bool = False,
    min_interval_seconds: int = 3600,
    consumer_active_ttl_days: int = 30,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    if (
        max_bytes < 1
        or min_interval_seconds < 0
        or consumer_active_ttl_days < 1
        or overflow_policy not in {
        "delete_oldest",
        "stop_recording",
        "fail_operation",
        }
    ):
        raise HelperError("Invalid cleanup policy")
    connection = connect(database_path, busy_timeout_ms)
    owner_id = f"cleanup-{uuid.uuid4()}"
    try:
        acquired = _cleanup_lease_acquire(connection, owner_id)
        if not acquired:
            return {
                "executed": False,
                "reason": "cleanup_in_progress",
                "database_now": _database_now(connection),
            }
        database_now = _database_now(connection)
        meta = _meta_dict(connection)
        last_cleanup_at = meta.get("last_cleanup_at")
        next_cleanup_not_before = _next_cleanup_time(
            connection,
            last_cleanup_at or database_now,
            min_interval_seconds,
        )
        if if_due and last_cleanup_at is not None and database_now < next_cleanup_not_before:
            return {
                "executed": False,
                "reason": "not_due",
                "database_now": database_now,
                "last_cleanup_at": last_cleanup_at,
                "next_cleanup_not_before": next_cleanup_not_before,
            }

        database_bytes_before = _database_sizes(connection)
        ttl_deleted = 0
        capacity_deleted = 0
        deleted_changes = 0
        protected_ack = _minimum_active_acknowledged_seq(
            connection,
            consumer_active_ttl_days,
        )
        with connection:
            if ttl_days is not None:
                cutoff = connection.execute(
                    """
                    SELECT strftime(
                        '%Y-%m-%dT%H:%M:%fZ',
                        'now',
                        '-' || ? || ' days'
                    )
                    """,
                    (ttl_days,),
                ).fetchone()[0]
                event_ids = _cleanup_candidates(
                    connection,
                    protected_ack=protected_ack,
                    completed_before=str(cutoff),
                )
                deleted_changes += _count_changes(connection, event_ids)
                ttl_deleted += _delete_operations(connection, event_ids)

            while _effective_database_bytes(connection) > max_bytes:
                if overflow_policy != "delete_oldest":
                    break
                event_ids = _cleanup_candidates(
                    connection,
                    protected_ack=protected_ack,
                    limit=1,
                )
                if not event_ids:
                    break
                deleted_changes += _count_changes(connection, event_ids)
                capacity_deleted += _delete_operations(connection, event_ids)

        bounds = connection.execute(
            "SELECT MIN(seq) AS min_seq, MAX(seq) AS max_seq FROM history_changes"
        ).fetchone()
        database_now = _database_now(connection)
        result = {
            "executed": True,
            "reason": "ttl_or_capacity",
            "database_now": database_now,
            "last_cleanup_at": database_now,
            "next_cleanup_not_before": _next_cleanup_time(
                connection,
                database_now,
                min_interval_seconds,
            ),
            "min_available_change_seq": int(bounds["min_seq"] or 0),
            "max_change_seq": int(bounds["max_seq"] or 0),
            "protected_acknowledged_seq": protected_ack,
            "ttl_deleted_operations": ttl_deleted,
            "capacity_deleted_operations": capacity_deleted,
            "deleted_operations": ttl_deleted + capacity_deleted,
            "deleted_changes": deleted_changes,
            "database_bytes_before": database_bytes_before["logical_database_bytes"],
            **_database_sizes(connection),
        }
        with connection:
            connection.executemany(
                "INSERT OR REPLACE INTO history_meta(key, value) VALUES (?, ?)",
                {
                    "last_cleanup_at": database_now,
                    "last_cleanup_result": canonical_json(result),
                    "last_cleanup_deleted_operations": str(result["deleted_operations"]),
                    "last_cleanup_database_bytes": str(result["logical_database_bytes"]),
                    "last_cleanup_reason": str(result["reason"]),
                }.items(),
            )
        return result
    finally:
        try:
            with connection:
                connection.execute(
                    "DELETE FROM history_leases WHERE resource_key = ? AND owner_id = ?",
                    ("history:cleanup", owner_id),
                )
        except sqlite3.Error:
            pass
        connection.close()


PERSISTED_CONFIG_FIELDS = {
    "ttl_days",
    "max_database_bytes",
    "max_operation_output_bytes",
    "overflow_policy",
    "capture_stdout",
    "capture_stderr",
    "output_chunk_bytes",
    "output_flush_bytes",
    "output_flush_interval_ms",
    "output_queue_max_bytes",
    "output_queue_max_chunks",
    "output_write_timeout_seconds",
    "compression",
    "compression_min_bytes",
    "compression_level",
    "sqlite_busy_timeout_ms",
    "sqlite_cache_bytes",
    "helper_query_max_bytes",
    "helper_envelope_max_bytes",
    "cleanup_interval_operations",
    "cleanup_on_connect",
    "cleanup_min_interval_seconds",
    "consumer_active_ttl_days",
    "stale_started_after_seconds",
}


def _meta_dict(connection: sqlite3.Connection) -> dict[str, str]:
    try:
        rows = connection.execute("SELECT key, value FROM history_meta").fetchall()
    except sqlite3.OperationalError:
        return {}
    return {str(row["key"]): str(row["value"]) for row in rows}


def _identity(connection: sqlite3.Connection) -> dict[str, str]:
    meta = _meta_dict(connection)
    required = ("provider_key", "sandbox_id", "sandbox_instance_id")
    missing = [key for key in required if not meta.get(key)]
    if missing:
        raise HelperError(f"Database identity is incomplete: {', '.join(missing)}")
    return {key: meta[key] for key in required}


def _required_dict(value: dict[str, Any], key: str) -> dict[str, Any]:
    return _object(value.get(key), key)


def _required_text(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise HelperError(f"{key} must be a non-empty string")
    return item


def _optional_json(value: Any) -> str | None:
    return None if value is None else canonical_json(value)


def _decode_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    return json.loads(str(value))


def _decode_json_object(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    return _object(json.loads(str(value)), "JSON value")


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise HelperError(f"{label} must be an object")
    return cast(dict[str, Any], value)


def _optional_object(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return _object(value, "value")


def _object_list(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise HelperError(f"{label} must be an array")
    return [_object(item, f"{label} item") for item in cast(list[Any], value)]


def _require_event(connection: sqlite3.Connection, event_id: str) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM history_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if row is None:
        raise HelperError(f"Operation does not exist: {event_id}")
    return row


def _append_change(
    connection: sqlite3.Connection,
    change_id: str,
    event_id: str,
    change_type: str,
    changed_at: str,
) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO history_changes(
            change_id, event_id, change_type, changed_at, schema_version
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (change_id, event_id, change_type, changed_at, SCHEMA_VERSION),
    )


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _effective_database_bytes(connection: sqlite3.Connection) -> int:
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
    free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return max(0, page_count - free_pages) * page_size


def _database_now(connection: sqlite3.Connection) -> str:
    return str(
        connection.execute(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
        ).fetchone()[0]
    )


def _next_cleanup_time(
    connection: sqlite3.Connection,
    from_time: str,
    min_interval_seconds: int,
) -> str:
    return str(
        connection.execute(
            """
            SELECT strftime(
                '%Y-%m-%dT%H:%M:%fZ',
                ?,
                '+' || ? || ' seconds'
            )
            """,
            (from_time, min_interval_seconds),
        ).fetchone()[0]
    )


def _cleanup_lease_acquire(connection: sqlite3.Connection, owner_id: str) -> bool:
    with connection:
        connection.execute(
            """
            INSERT INTO history_leases(resource_key, owner_id, expires_at, updated_at)
            VALUES (
                'history:cleanup',
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+5 minutes'),
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ON CONFLICT(resource_key) DO UPDATE SET
                owner_id = excluded.owner_id,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            WHERE history_leases.owner_id = excluded.owner_id
               OR history_leases.expires_at <= strftime(
                    '%Y-%m-%dT%H:%M:%fZ', 'now'
               )
            """,
            (owner_id,),
        )
    row = connection.execute(
        "SELECT owner_id FROM history_leases WHERE resource_key = 'history:cleanup'"
    ).fetchone()
    return row is not None and row["owner_id"] == owner_id


def _minimum_active_acknowledged_seq(
    connection: sqlite3.Connection,
    consumer_active_ttl_days: int,
) -> int | None:
    row = connection.execute(
        """
        SELECT MIN(acknowledged_seq) AS acknowledged_seq
        FROM history_consumers
        WHERE updated_at >= strftime(
            '%Y-%m-%dT%H:%M:%fZ',
            'now',
            '-' || ? || ' days'
        )
        """,
        (consumer_active_ttl_days,),
    ).fetchone()
    value = row["acknowledged_seq"]
    return int(value) if value is not None else None


def _cleanup_candidates(
    connection: sqlite3.Connection,
    *,
    protected_ack: int | None,
    completed_before: str | None = None,
    limit: int | None = None,
) -> list[str]:
    parameters: list[Any] = []
    conditions = [
        "events.status != 'started'",
        "events.completed_at IS NOT NULL",
        "(commands.event_id IS NULL OR commands.history_storage_state IN "
        "('complete', 'truncated_by_policy', 'metadata_only'))",
    ]
    if completed_before is not None:
        conditions.append("events.completed_at < ?")
        parameters.append(completed_before)
    if protected_ack is not None:
        conditions.append(
            "COALESCE((SELECT MAX(changes.seq) FROM history_changes changes "
            "WHERE changes.event_id = events.event_id), 0) <= ?"
        )
        parameters.append(protected_ack)
    sql = f"""
        SELECT events.event_id
        FROM history_events events
        LEFT JOIN command_executions commands ON commands.event_id = events.event_id
        WHERE {' AND '.join(conditions)}
        ORDER BY events.seq
    """
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    rows = connection.execute(sql, parameters).fetchall()
    return [str(row["event_id"]) for row in rows]


def _count_changes(connection: sqlite3.Connection, event_ids: list[str]) -> int:
    if not event_ids:
        return 0
    placeholders = ",".join("?" for _ in event_ids)
    row = connection.execute(
        f"SELECT COUNT(*) FROM history_changes WHERE event_id IN ({placeholders})",
        event_ids,
    ).fetchone()
    return int(row[0])


def _database_sizes(connection: sqlite3.Connection) -> dict[str, int]:
    database_path = Path(str(connection.execute("PRAGMA database_list").fetchone()[2]))
    main_bytes = database_path.stat().st_size if database_path.exists() else 0
    wal_path = Path(f"{database_path}-wal")
    shm_path = Path(f"{database_path}-shm")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    shm_bytes = shm_path.stat().st_size if shm_path.exists() else 0
    return {
        "logical_database_bytes": _effective_database_bytes(connection),
        "physical_database_bytes": main_bytes + wal_bytes,
        "main_database_bytes": main_bytes,
        "wal_bytes": wal_bytes,
        "shm_bytes": shm_bytes,
    }


def _delete_operations(connection: sqlite3.Connection, event_ids: list[str]) -> int:
    if not event_ids:
        return 0
    placeholders = ",".join("?" for _ in event_ids)
    connection.execute(
        f"DELETE FROM history_changes WHERE event_id IN ({placeholders})",
        event_ids,
    )
    cursor = connection.execute(
        f"DELETE FROM history_events WHERE event_id IN ({placeholders})",
        event_ids,
    )
    return int(cursor.rowcount)


def _load_json_file(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    return _object(value, f"JSON file {path}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="history_helper.py")
    parser.add_argument("--database", default=DEFAULT_DATABASE_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument("--identity-file", required=True)

    subparsers.add_parser("health")
    subparsers.add_parser("identity-get")
    subparsers.add_parser("config-get")

    config_set_parser = subparsers.add_parser("config-set")
    config_set_parser.add_argument("--config-file", required=True)
    config_set_parser.add_argument("--expected-revision", type=int, required=True)

    apply_parser = subparsers.add_parser("apply")
    apply_parser.add_argument("--batch-file", required=True)

    query_parser = subparsers.add_parser("query")
    query_parser.add_argument("--after-change-seq", type=int, required=True)
    query_parser.add_argument("--limit", type=int, required=True)
    query_parser.add_argument("--max-bytes", type=int, required=True)

    operation_parser = subparsers.add_parser("get-operation")
    operation_parser.add_argument("--event-id", required=True)

    output_parser = subparsers.add_parser("get-output")
    output_parser.add_argument("--event-id", required=True)
    output_parser.add_argument("--stream", choices=("stdout", "stderr"), required=True)
    output_parser.add_argument("--after-chunk", type=int, default=-1)

    ack_parser = subparsers.add_parser("ack")
    ack_parser.add_argument("--consumer-id", required=True)
    ack_parser.add_argument("--seq", type=int, required=True)

    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--ttl-days", type=int)
    cleanup_parser.add_argument("--max-bytes", type=int, required=True)
    cleanup_parser.add_argument("--if-due", action="store_true")
    cleanup_parser.add_argument("--min-interval-seconds", type=int, default=3600)
    cleanup_parser.add_argument("--consumer-active-ttl-days", type=int, default=30)
    cleanup_parser.add_argument(
        "--overflow-policy",
        choices=("delete_oldest", "stop_recording", "fail_operation"),
        required=True,
    )

    for name in ("lease-acquire", "lease-renew"):
        lease_parser = subparsers.add_parser(name)
        lease_parser.add_argument("--resource-key", required=True)
        lease_parser.add_argument("--owner-id", required=True)
        lease_parser.add_argument("--ttl-ms", type=int, required=True)

    release_parser = subparsers.add_parser("lease-release")
    release_parser.add_argument("--resource-key", required=True)
    release_parser.add_argument("--owner-id", required=True)
    return parser


def _dispatch(arguments: argparse.Namespace) -> dict[str, Any]:
    database = str(arguments.database)
    command = str(arguments.command)
    if command == "init":
        identity_file = _load_json_file(str(arguments.identity_file))
        config = identity_file.pop("history_config", None)
        return initialize(database, identity_file, config)
    if command == "health":
        return health(database)
    if command == "identity-get":
        return health(database)["identity"]
    if command == "config-get":
        return config_get(database)
    if command == "config-set":
        payload = _load_json_file(str(arguments.config_file))
        changes = _object(payload.get("changes"), "config-set changes")
        return config_set(
            database,
            changes,
            expected_revision=int(arguments.expected_revision),
            updated_by=_required_text(payload, "updated_by"),
            allowed_fields=PERSISTED_CONFIG_FIELDS,
        )
    if command == "apply":
        return apply_batch(database, _load_json_file(str(arguments.batch_file)))
    if command == "query":
        return query_changes(
            database,
            after_change_seq=int(arguments.after_change_seq),
            limit=int(arguments.limit),
            max_bytes=int(arguments.max_bytes),
        )
    if command == "get-operation":
        return get_operation(database, str(arguments.event_id))
    if command == "get-output":
        return get_output(
            database,
            str(arguments.event_id),
            str(arguments.stream),
            after_chunk=int(arguments.after_chunk),
        )
    if command == "ack":
        return acknowledge(database, str(arguments.consumer_id), int(arguments.seq))
    if command == "cleanup":
        return cleanup(
            database,
            ttl_days=arguments.ttl_days,
            max_bytes=int(arguments.max_bytes),
            overflow_policy=str(arguments.overflow_policy),
            if_due=bool(arguments.if_due),
            min_interval_seconds=int(arguments.min_interval_seconds),
            consumer_active_ttl_days=int(arguments.consumer_active_ttl_days),
        )
    if command == "lease-acquire":
        return lease_acquire(
            database,
            str(arguments.resource_key),
            str(arguments.owner_id),
            int(arguments.ttl_ms),
        )
    if command == "lease-renew":
        return lease_renew(
            database,
            str(arguments.resource_key),
            str(arguments.owner_id),
            int(arguments.ttl_ms),
        )
    if command == "lease-release":
        return lease_release(
            database,
            str(arguments.resource_key),
            str(arguments.owner_id),
        )
    raise HelperError(f"Unknown command: {command}")


def main(argv: list[str] | None = None) -> int:
    request_id = str(uuid.uuid4())
    try:
        arguments = _build_parser().parse_args(argv)
        data = _dispatch(arguments)
        envelope = {
            "ok": True,
            "schema_version": SCHEMA_VERSION,
            "request_id": request_id,
            "data": data,
        }
        print(canonical_json(envelope))
        return 0
    except Exception as error:
        envelope = {
            "ok": False,
            "schema_version": SCHEMA_VERSION,
            "request_id": request_id,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
        }
        print(canonical_json(envelope))
        print(f"history-helper: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
