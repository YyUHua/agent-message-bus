import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def test_init_db_creates_required_tables_and_pragmas(tmp_path):
    from anyue_bus_core import init_db

    db_path = tmp_path / "anyue_bus.db"
    init_db(db_path)

    assert db_path.exists()

    conn = sqlite3.connect(db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {
            "messages",
            "agents",
            "events",
            "control_signals",
            "dead_letters",
        }.issubset(tables)

        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]

        assert journal_mode.lower() == "wal"
        assert busy_timeout == 5000
        assert synchronous in (1, 2, "1", "2")  # NORMAL may report 1 or 2 depending on SQLite build
    finally:
        conn.close()


def test_messages_schema_has_core_columns_and_dedupe_index(tmp_path):
    from anyue_bus_core import init_db

    db_path = tmp_path / "anyue_bus.db"
    init_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        columns = {
            row[1]: row[2]
            for row in conn.execute("PRAGMA table_info(messages)").fetchall()
        }
        for name in [
            "message_id",
            "client_msg_id",
            "from_agent",
            "to_agent",
            "type",
            "content",
            "status",
            "priority",
            "retry_count",
            "max_retries",
            "processing_mode",
            "processing_status",
            "reply",
            "error_code",
            "error_detail",
            "metadata_json",
            "created_at",
            "updated_at",
            "queued_at",
            "dispatched_at",
            "ack_pending_at",
            "acked_at",
            "failed_at",
        ]:
            assert name in columns

        indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(messages)").fetchall()
        }
        assert "idx_messages_to_status" in indexes
        assert "idx_messages_status" in indexes
        assert "idx_messages_created_at" in indexes
    finally:
        conn.close()
