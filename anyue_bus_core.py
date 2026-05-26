from pathlib import Path
from typing import Optional, Union
import json
import re
import sqlite3
import time
import uuid

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA cache_size=-2000;

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    client_msg_id TEXT,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'chat',
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 5,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    processing_mode TEXT,
    processing_status TEXT,
    reply TEXT,
    error_code TEXT,
    error_detail TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    queued_at INTEGER,
    dispatched_at INTEGER,
    ack_pending_at INTEGER,
    acked_at INTEGER,
    failed_at INTEGER,
    UNIQUE(from_agent, client_msg_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_to_status
ON messages(to_agent, status, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_status
ON messages(status);

CREATE INDEX IF NOT EXISTS idx_messages_created_at
ON messages(created_at);

CREATE TABLE IF NOT EXISTS agents (
    agent_id TEXT PRIMARY KEY,
    adapter_url TEXT,
    native_url TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    native_status TEXT NOT NULL DEFAULT 'unknown',
    last_heartbeat INTEGER,
    stop_requested INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    message_id TEXT,
    event_type TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    detail TEXT,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS control_signals (
    signal_id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    reason TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    cleared_at INTEGER
);

CREATE TABLE IF NOT EXISTS dead_letters (
    message_id TEXT PRIMARY KEY,
    final_status TEXT NOT NULL,
    reason TEXT,
    created_at INTEGER NOT NULL,
    acknowledged_at INTEGER
);
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(obj) -> str:
    return json.dumps(obj or {}, ensure_ascii=False, separators=(",", ":"))


def _extract_agent_from_detail(detail: Optional[str]) -> Optional[str]:
    if not detail:
        return None
    match = re.search(r"(?:polled_by|ack_pending_by|acked_by|fallback_by|failed_by|requeued_by)=([^;]+)", detail)
    if match:
        return match.group(1)
    return None


def _format_event_summary(row: sqlite3.Row) -> str:
    new_status = row["new_status"] or row["event_type"] or "event"
    detail = row["detail"]
    if detail:
        return f"{new_status} · {detail}"
    return str(new_status)


def init_db(db_path: Union[str, Path]) -> Path:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()
    return db_path


def send_message(
    db_path: Union[str, Path],
    from_agent: str,
    to_agent: str,
    content: str,
    *,
    type: str = "chat",
    client_msg_id: Optional[str] = None,
    priority: int = 5,
    metadata: Optional[dict] = None,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        if client_msg_id:
            existing = cur.execute(
                "SELECT message_id FROM messages WHERE from_agent = ? AND client_msg_id = ?",
                (from_agent, client_msg_id),
            ).fetchone()
            if existing:
                return {
                    "ok": True,
                    "message_id": existing["message_id"],
                    "status": "DUPLICATE",
                }

        message_id = str(uuid.uuid4())
        metadata_json = _json_dumps(metadata)
        cur.execute(
            """
            INSERT INTO messages (
                message_id, client_msg_id, from_agent, to_agent, type, content, status,
                priority, retry_count, max_retries, processing_mode, processing_status,
                reply, error_code, error_detail, metadata_json,
                created_at, updated_at, queued_at, dispatched_at, ack_pending_at, acked_at, failed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                client_msg_id,
                from_agent,
                to_agent,
                type,
                content,
                "QUEUED",
                priority,
                0,
                3,
                None,
                None,
                None,
                None,
                None,
                metadata_json,
                now,
                now,
                now,
                None,
                None,
                None,
                None,
            ),
        )
        conn.commit()
        return {"ok": True, "message_id": message_id, "status": "QUEUED"}
    finally:
        conn.close()


def poll_messages(
    db_path: Union[str, Path],
    agent: str,
    *,
    limit: int = 1,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT * FROM messages
            WHERE to_agent = ? AND status = 'QUEUED'
            ORDER BY priority DESC, created_at ASC
            LIMIT ?
            """,
            (agent, limit),
        ).fetchall()
        messages = []
        for row in rows:
            conn.execute(
                """
                UPDATE messages
                SET status = 'DISPATCHED', updated_at = ?, dispatched_at = ?
                WHERE message_id = ? AND status = 'QUEUED'
                """,
                (now, now, row["message_id"]),
            )
            conn.execute(
                """
                INSERT INTO events (event_id, message_id, event_type, old_status, new_status, detail, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    row["message_id"],
                    "status_change",
                    "QUEUED",
                    "DISPATCHED",
                    f"polled_by={agent}",
                    now,
                ),
            )
            messages.append(
                {
                    "message_id": row["message_id"],
                    "from": row["from_agent"],
                    "to": row["to_agent"],
                    "type": row["type"],
                    "content": row["content"],
                    "status": "DISPATCHED",
                    "priority": row["priority"],
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
            )
        conn.commit()
        return {"ok": True, "system_state": "idle", "messages": messages}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ack_pending(
    db_path: Union[str, Path],
    message_id: str,
    agent: str,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT message_id FROM messages
            WHERE message_id = ? AND to_agent = ? AND status = 'DISPATCHED'
            """,
            (message_id, agent),
        ).fetchone()
        if not row:
            conn.rollback()
            return {"ok": False, "message_id": message_id, "status": "NOT_FOUND_OR_NOT_DISPATCHED"}

        conn.execute(
            """
            UPDATE messages
            SET status = 'ACK_PENDING', updated_at = ?, ack_pending_at = ?
            WHERE message_id = ? AND status = 'DISPATCHED'
            """,
            (now, now, message_id),
        )
        conn.execute(
            """
            INSERT INTO events (event_id, message_id, event_type, old_status, new_status, detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                message_id,
                "status_change",
                "DISPATCHED",
                "ACK_PENDING",
                f"ack_pending_by={agent}",
                now,
            ),
        )
        conn.commit()
        return {"ok": True, "message_id": message_id, "status": "ACK_PENDING"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ack_message(
    db_path: Union[str, Path],
    message_id: str,
    agent: str,
    *,
    reply: Optional[str] = None,
    processing_mode: Optional[str] = None,
    error_detail: Optional[str] = None,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT message_id FROM messages
            WHERE message_id = ? AND to_agent = ? AND status = 'ACK_PENDING'
            """,
            (message_id, agent),
        ).fetchone()
        if not row:
            conn.rollback()
            return {"ok": False, "message_id": message_id, "status": "NOT_FOUND_OR_NOT_ACK_PENDING"}

        mode = processing_mode or "ai"
        if mode not in ("ai", "explicit_echo", "fallback"):
            conn.rollback()
            return {"ok": False, "message_id": message_id, "status": "INVALID_PROCESSING_MODE"}

        if mode == "ai":
            conn.execute(
                """
                UPDATE messages
                SET status = 'ACKED', updated_at = ?, acked_at = ?, reply = ?,
                    processing_mode = ?, processing_status = 'done', error_detail = NULL
                WHERE message_id = ? AND status = 'ACK_PENDING'
                """,
                (now, now, reply, mode, message_id),
            )
            old_status, new_status, detail = "ACK_PENDING", "ACKED", f"acked_by={agent};mode={mode}"
            result_status = "ACKED"
        elif mode == "explicit_echo":
            conn.execute(
                """
                UPDATE messages
                SET status = 'FALLBACK_RECEIVED', updated_at = ?, reply = ?,
                    processing_mode = ?, processing_status = 'fallback', error_detail = ?
                WHERE message_id = ? AND status = 'ACK_PENDING'
                """,
                (now, reply, mode, error_detail, message_id),
            )
            old_status, new_status, detail = "ACK_PENDING", "FALLBACK_RECEIVED", f"fallback_by={agent};mode={mode}"
            result_status = "FALLBACK_RECEIVED"
        else:
            cur = conn.execute(
                """
                UPDATE messages
                SET status = 'QUEUED', updated_at = ?, ack_pending_at = NULL, dispatched_at = NULL,
                    retry_count = retry_count + 1, processing_mode = ?, processing_status = 'retrying',
                    error_detail = ?
                WHERE message_id = ? AND status = 'ACK_PENDING' AND retry_count < max_retries
                """,
                (now, mode, error_detail or "worker fallback", message_id),
            )
            if cur.rowcount == 0:
                conn.execute(
                    """
                    UPDATE messages
                    SET status = 'FAILED', updated_at = ?, failed_at = ?, processing_mode = ?,
                        processing_status = 'failed', error_detail = ?
                    WHERE message_id = ? AND status = 'ACK_PENDING'
                    """,
                    (now, now, mode, error_detail or "worker fallback exceeded retries", message_id),
                )
                conn.execute(
                    """
                    INSERT INTO dead_letters (message_id, final_status, reason, created_at, acknowledged_at)
                    VALUES (?, 'FAILED', ?, ?, NULL)
                    ON CONFLICT(message_id) DO UPDATE SET
                        final_status=excluded.final_status, reason=excluded.reason,
                        created_at=excluded.created_at, acknowledged_at=NULL
                    """,
                    (message_id, error_detail or "worker fallback exceeded retries", now),
                )
                old_status, new_status, detail = "ACK_PENDING", "FAILED", f"failed_by={agent};mode={mode}"
                result_status = "FAILED"
            else:
                old_status, new_status, detail = "ACK_PENDING", "QUEUED", f"requeued_by={agent};mode={mode}"
                result_status = "QUEUED"

        conn.execute(
            """
            INSERT INTO events (event_id, message_id, event_type, old_status, new_status, detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), message_id, "status_change", old_status, new_status, detail, now),
        )
        conn.commit()
        return {"ok": True, "message_id": message_id, "status": result_status}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def requeue_stale_messages(db_path: Union[str, Path], *, stale_ms: int = 120000):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    cutoff = now - stale_ms
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT message_id, retry_count, max_retries, ack_pending_at
            FROM messages
            WHERE status = 'ACK_PENDING' AND ack_pending_at IS NOT NULL AND ack_pending_at < ?
            ORDER BY ack_pending_at ASC
            """,
            (cutoff,),
        ).fetchall()
        requeued = 0
        dead_lettered = 0
        for row in rows:
            mid = row["message_id"]
            if row["retry_count"] < row["max_retries"]:
                conn.execute(
                    """
                    UPDATE messages
                    SET status = 'QUEUED', updated_at = ?, dispatched_at = NULL, ack_pending_at = NULL,
                        retry_count = retry_count + 1, processing_status = 'retrying',
                        error_detail = 'ACK_PENDING_TIMEOUT'
                    WHERE message_id = ? AND status = 'ACK_PENDING'
                    """,
                    (now, mid),
                )
                new_status = "QUEUED"
                requeued += 1
            else:
                reason = f"ACK_PENDING_TIMEOUT retry_count={row['retry_count']} max_retries={row['max_retries']}"
                conn.execute(
                    """
                    UPDATE messages
                    SET status = 'FAILED', updated_at = ?, failed_at = ?, processing_status = 'failed',
                        error_detail = ?
                    WHERE message_id = ? AND status = 'ACK_PENDING'
                    """,
                    (now, now, reason, mid),
                )
                conn.execute(
                    """
                    INSERT INTO dead_letters (message_id, final_status, reason, created_at, acknowledged_at)
                    VALUES (?, 'FAILED', ?, ?, NULL)
                    ON CONFLICT(message_id) DO UPDATE SET
                        final_status=excluded.final_status, reason=excluded.reason,
                        created_at=excluded.created_at, acknowledged_at=NULL
                    """,
                    (mid, reason, now),
                )
                new_status = "FAILED"
                dead_lettered += 1
            conn.execute(
                """
                INSERT INTO events (event_id, message_id, event_type, old_status, new_status, detail, created_at)
                VALUES (?, ?, 'status_change', 'ACK_PENDING', ?, 'ACK_PENDING_TIMEOUT', ?)
                """,
                (str(uuid.uuid4()), mid, new_status, now),
            )
        conn.commit()
        return {"ok": True, "checked": len(rows), "requeued": requeued, "dead_lettered": dead_lettered}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_control_signal(
    db_path: Union[str, Path],
    command: str,
    requested_by: str,
    *,
    reason: Optional[str] = None,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    signal_id = str(uuid.uuid4())
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO control_signals (signal_id, command, requested_by, reason, active, created_at, cleared_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (signal_id, command, requested_by, reason, 1, now, None),
        )
        conn.commit()
        return {"ok": True, "signal_id": signal_id, "command": command, "active": True}
    finally:
        conn.close()


def get_active_control_signals(db_path: Union[str, Path]):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT signal_id, command, requested_by, reason, active, created_at, cleared_at
            FROM control_signals
            WHERE active = 1
            ORDER BY created_at ASC
            """
        ).fetchall()
        signals = [
            {
                "signal_id": row["signal_id"],
                "command": row["command"],
                "requested_by": row["requested_by"],
                "reason": row["reason"],
                "active": bool(row["active"]),
                "created_at": row["created_at"],
                "cleared_at": row["cleared_at"],
            }
            for row in rows
        ]
        return {"ok": True, "signals": signals}
    finally:
        conn.close()


def clear_control_signal(db_path: Union[str, Path], signal_id: str):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            """
            UPDATE control_signals
            SET active = 0, cleared_at = ?
            WHERE signal_id = ? AND active = 1
            """,
            (now, signal_id),
        )
        if cur.rowcount == 0:
            conn.rollback()
            return {"ok": False, "signal_id": signal_id, "status": "NOT_FOUND_OR_ALREADY_CLEARED"}
        conn.commit()
        return {"ok": True, "signal_id": signal_id, "active": False}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def record_heartbeat(
    db_path: Union[str, Path],
    agent: str,
    *,
    adapter_url: Optional[str] = None,
    native_url: Optional[str] = None,
    status: str = "unknown",
    native_status: str = "unknown",
    metadata: Optional[dict] = None,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO agents (agent_id, adapter_url, native_url, status, native_status, last_heartbeat, stop_requested, metadata_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(agent_id) DO UPDATE SET
                adapter_url=excluded.adapter_url,
                native_url=excluded.native_url,
                status=excluded.status,
                native_status=excluded.native_status,
                last_heartbeat=excluded.last_heartbeat,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            (agent, adapter_url, native_url, status, native_status, now, _json_dumps(metadata), now),
        )
        conn.commit()
        return {"ok": True, "agent": agent, "status": status}
    finally:
        conn.close()


def get_status(db_path: Union[str, Path]):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        message_counts = {
            row[0]: row[1]
            for row in conn.execute("SELECT status, COUNT(*) FROM messages GROUP BY status")
        }
        agents = [dict(row) for row in conn.execute("SELECT agent_id, adapter_url, native_url, status, native_status, last_heartbeat, stop_requested, metadata_json, updated_at FROM agents ORDER BY agent_id ASC")]
        controls = [
            {
                "signal_id": row["signal_id"],
                "command": row["command"],
                "requested_by": row["requested_by"],
                "reason": row["reason"],
                "active": bool(row["active"]),
                "created_at": row["created_at"],
                "cleared_at": row["cleared_at"],
            }
            for row in conn.execute("SELECT signal_id, command, requested_by, reason, active, created_at, cleared_at FROM control_signals WHERE active = 1 ORDER BY created_at ASC")
        ]
        stuck_messages = []
        for row in conn.execute(
            """
            SELECT message_id, from_agent, to_agent, content, status, retry_count, max_retries,
                   processing_mode, processing_status, error_detail, updated_at, ack_pending_at
            FROM messages
            WHERE status IN ('DISPATCHED', 'ACK_PENDING', 'FALLBACK_RECEIVED', 'FAILED')
            ORDER BY updated_at DESC
            LIMIT 20
            """
        ):
            stuck_messages.append(dict(row))
        return {"ok": True, "message_counts": message_counts, "agents": agents, "active_controls": controls, "stuck_messages": stuck_messages}
    finally:
        conn.close()


def get_events(db_path: Union[str, Path], *, after_id: int = 0, limit: int = 50):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT rowid AS event_id, message_id, event_type, old_status, new_status, detail, created_at
            FROM events
            WHERE rowid > ?
            ORDER BY rowid ASC
            LIMIT ?
            """,
            (max(after_id, 0), max(limit, 1)),
        ).fetchall()
        events = [
            {
                "event_id": row["event_id"],
                "message_id": row["message_id"],
                "type": row["event_type"],
                "agent": _extract_agent_from_detail(row["detail"]),
                "summary": _format_event_summary(row),
                "ts": row["created_at"],
                "old_status": row["old_status"],
                "new_status": row["new_status"],
                "detail": row["detail"],
            }
            for row in rows
        ]
        return {"ok": True, "events": events}
    finally:
        conn.close()


def get_replies(db_path: Union[str, Path], *, since: int = 0, limit: int = 20):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT message_id, from_agent, to_agent, content, reply, status, created_at, acked_at, updated_at
            FROM messages
            WHERE status IN ('ACKED', 'FALLBACK_RECEIVED')
              AND reply IS NOT NULL
              AND reply != ''
              AND COALESCE(acked_at, updated_at, created_at) > ?
            ORDER BY COALESCE(acked_at, updated_at, created_at) ASC
            LIMIT ?
            """,
            (max(since, 0), max(limit, 1)),
        ).fetchall()
        replies = [
            {
                "message_id": row["message_id"],
                "from_agent": row["from_agent"],
                "to_agent": row["to_agent"],
                "content": row["content"],
                "reply": row["reply"],
                "status": row["status"],
                "created_at": row["created_at"],
                "acked_at": row["acked_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
        return {"ok": True, "replies": replies}
    finally:
        conn.close()


def get_dashboard(db_path: Union[str, Path], *, recent_limit: int = 10):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        status = get_status(db_path)
        recent_messages = []
        for row in conn.execute(
            """
            SELECT message_id, from_agent, to_agent, content, status, reply, metadata_json,
                   retry_count, max_retries, processing_mode, processing_status, error_detail,
                   created_at, updated_at, dispatched_at, ack_pending_at, acked_at
            FROM messages
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (recent_limit,),
        ):
            item = dict(row)
            try:
                item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
            except Exception:
                item["metadata"] = {}
            recent_messages.append(item)
        processing_messages = [
            item for item in recent_messages
            if item["status"] in ("DISPATCHED", "ACK_PENDING", "QUEUED")
        ]
        stuck_messages = [
            item for item in recent_messages
            if item["status"] in ("DISPATCHED", "ACK_PENDING", "FALLBACK_RECEIVED", "FAILED")
        ]
        return {
            "ok": True,
            "status": status,
            "recent_messages": recent_messages,
            "processing_messages": processing_messages,
            "stuck_messages": stuck_messages,
        }
    finally:
        conn.close()


def get_health(db_path: Union[str, Path]):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        wal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        return {"ok": True, "database": "ok", "wal_enabled": wal_mode.lower() == "wal"}
    finally:
        conn.close()


def move_to_dead_letter(
    db_path: Union[str, Path],
    message_id: str,
    *,
    reason: str,
):
    db_path = Path(db_path)
    init_db(db_path)
    now = _now_ms()
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT message_id FROM messages WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        if not row:
            conn.rollback()
            return {"ok": False, "message_id": message_id, "status": "NOT_FOUND"}
        conn.execute(
            """
            UPDATE messages
            SET status = 'FAILED', updated_at = ?, failed_at = ?, error_detail = ?
            WHERE message_id = ?
            """,
            (now, now, reason, message_id),
        )
        conn.execute(
            """
            INSERT INTO dead_letters (message_id, final_status, reason, created_at, acknowledged_at)
            VALUES (?, 'FAILED', ?, ?, NULL)
            ON CONFLICT(message_id) DO UPDATE SET
                final_status=excluded.final_status,
                reason=excluded.reason,
                created_at=excluded.created_at,
                acknowledged_at=NULL
            """,
            (message_id, reason, now),
        )
        conn.execute(
            """
            INSERT INTO events (event_id, message_id, event_type, old_status, new_status, detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                message_id,
                "status_change",
                None,
                "FAILED",
                reason,
                now,
            ),
        )
        conn.commit()
        return {"ok": True, "message_id": message_id, "status": "FAILED"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_dead_letters(db_path: Union[str, Path]):
    db_path = Path(db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        dead_letters = [dict(row) for row in conn.execute("SELECT message_id, final_status, reason, created_at, acknowledged_at FROM dead_letters ORDER BY created_at ASC")]
        return {"ok": True, "dead_letters": dead_letters}
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Initialize Anyue Bus SQLite database")
    parser.add_argument("db_path", nargs="?", default="./anyue_bus.db")
    args = parser.parse_args()
    path = init_db(args.db_path)
    print(path)
