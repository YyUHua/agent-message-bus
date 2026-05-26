import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_send_message_inserts_queued_row_and_returns_message_id(tmp_path):
    from anyue_bus_core import init_db, send_message

    db_path = tmp_path / "anyue_bus.db"
    init_db(db_path)

    result = send_message(
        db_path,
        from_agent="agent_a",
        to_agent="agent_b",
        content="你好，Agent B。",
        client_msg_id="client-001",
        priority=7,
        metadata={"trace_id": "trace-001", "is_internal": True},
    )

    assert result["ok"] is True
    assert result["status"] == "QUEUED"
    assert result["message_id"]

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT message_id, from_agent, to_agent, content, status, priority, metadata_json FROM messages WHERE message_id = ?",
            (result["message_id"],),
        ).fetchone()
        assert row is not None
        assert row[1:] == (
            "agent_a",
            "agent_b",
            "你好，Agent B。",
            "QUEUED",
            7,
            json.dumps({"trace_id": "trace-001", "is_internal": True}, ensure_ascii=False, separators=(",", ":")),
        )
    finally:
        conn.close()


def test_send_message_dedupes_client_msg_id(tmp_path):
    from anyue_bus_core import init_db, send_message

    db_path = tmp_path / "anyue_bus.db"
    init_db(db_path)

    first = send_message(db_path, "agent_a", "agent_b", "第一条", client_msg_id="dup-1")
    second = send_message(db_path, "agent_a", "agent_b", "第二条", client_msg_id="dup-1")

    assert first["message_id"] == second["message_id"]
    assert second["status"] == "DUPLICATE"

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM messages WHERE from_agent = 'agent_a' AND client_msg_id = 'dup-1'").fetchone()[0]
        assert count == 1
    finally:
        conn.close()
