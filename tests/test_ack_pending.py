import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _dispatched_message(db_path, content='ack-pending-1'):
    from anyue_bus_core import send_message, poll_messages

    sent = send_message(
        db_path,
        from_agent='agent_a',
        to_agent='agent_b',
        content=content,
        client_msg_id=f'seed-{content}',
        metadata={'trace_id': f'trace-{content}', 'is_internal': True},
    )
    polled = poll_messages(db_path, agent='agent_b', limit=1)
    assert polled['messages'][0]['message_id'] == sent['message_id']
    return sent


def test_ack_pending_moves_dispatched_message_to_ack_pending(tmp_path):
    from anyue_bus_core import init_db, ack_pending

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _dispatched_message(db_path)

    result = ack_pending(db_path, message_id=seed['message_id'], agent='agent_b')

    assert result == {'ok': True, 'message_id': seed['message_id'], 'status': 'ACK_PENDING'}

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT status, ack_pending_at FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()
        assert row[0] == 'ACK_PENDING'
        assert row[1] is not None
        event = conn.execute(
            'SELECT old_status, new_status FROM events WHERE message_id = ? ORDER BY created_at DESC LIMIT 1',
            (seed['message_id'],),
        ).fetchone()
        assert event == ('DISPATCHED', 'ACK_PENDING')
    finally:
        conn.close()


def test_ack_pending_rejects_wrong_agent(tmp_path):
    from anyue_bus_core import init_db, ack_pending

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _dispatched_message(db_path)

    result = ack_pending(db_path, message_id=seed['message_id'], agent='agent_a')

    assert result['ok'] is False
    assert result['status'] == 'NOT_FOUND_OR_NOT_DISPATCHED'

    conn = sqlite3.connect(db_path)
    try:
        status = conn.execute('SELECT status FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()[0]
        assert status == 'DISPATCHED'
    finally:
        conn.close()


def test_requeue_stale_ack_pending_moves_message_back_to_queued(tmp_path):
    from anyue_bus_core import init_db, ack_pending, requeue_stale_messages

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _dispatched_message(db_path, content='stale-requeue')
    ack_pending(db_path, message_id=seed['message_id'], agent='agent_b')

    conn = sqlite3.connect(db_path)
    try:
        stale_at = int(time.time() * 1000) - 60000
        conn.execute('UPDATE messages SET ack_pending_at = ?, updated_at = ? WHERE message_id = ?', (stale_at, stale_at, seed['message_id']))
        conn.commit()
    finally:
        conn.close()

    result = requeue_stale_messages(db_path, stale_ms=1000)

    assert result['ok'] is True
    assert result['requeued'] == 1
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT status, retry_count, ack_pending_at FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()
        assert row[0] == 'QUEUED'
        assert row[1] == 1
        assert row[2] is None
    finally:
        conn.close()


def test_requeue_stale_ack_pending_sends_over_retry_to_dead_letter(tmp_path):
    from anyue_bus_core import init_db, ack_pending, requeue_stale_messages

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _dispatched_message(db_path, content='stale-dead')
    ack_pending(db_path, message_id=seed['message_id'], agent='agent_b')

    conn = sqlite3.connect(db_path)
    try:
        stale_at = int(time.time() * 1000) - 60000
        conn.execute('UPDATE messages SET ack_pending_at = ?, updated_at = ?, retry_count = ?, max_retries = ? WHERE message_id = ?', (stale_at, stale_at, 3, 3, seed['message_id']))
        conn.commit()
    finally:
        conn.close()

    result = requeue_stale_messages(db_path, stale_ms=1000)

    assert result['dead_lettered'] == 1
    conn = sqlite3.connect(db_path)
    try:
        status = conn.execute('SELECT status FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()[0]
        dead = conn.execute('SELECT reason FROM dead_letters WHERE message_id = ?', (seed['message_id'],)).fetchone()[0]
        assert status == 'FAILED'
        assert 'ACK_PENDING_TIMEOUT' in dead
    finally:
        conn.close()
