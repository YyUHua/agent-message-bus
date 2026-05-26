import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _ack_pending_message(db_path, content='ack-1'):
    from anyue_bus_core import send_message, poll_messages, ack_pending

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
    pending = ack_pending(db_path, message_id=sent['message_id'], agent='agent_b')
    assert pending['status'] == 'ACK_PENDING'
    return sent


def test_ack_moves_ack_pending_message_to_acked_with_reply(tmp_path):
    from anyue_bus_core import init_db, ack_message

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _ack_pending_message(db_path)

    result = ack_message(db_path, message_id=seed['message_id'], agent='agent_b', reply='收到，开始处理')

    assert result == {'ok': True, 'message_id': seed['message_id'], 'status': 'ACKED'}

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT status, acked_at, reply, processing_mode FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()
        assert row[0] == 'ACKED'
        assert row[1] is not None
        assert row[2] == '收到，开始处理'
        assert row[3] == 'ai'
        event = conn.execute(
            'SELECT old_status, new_status FROM events WHERE message_id = ? ORDER BY created_at DESC LIMIT 1',
            (seed['message_id'],),
        ).fetchone()
        assert event == ('ACK_PENDING', 'ACKED')
    finally:
        conn.close()


def test_ack_rejects_message_not_in_ack_pending(tmp_path):
    from anyue_bus_core import init_db, send_message, ack_message

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    sent = send_message(db_path, from_agent='agent_a', to_agent='agent_b', content='还没领取', client_msg_id='not-ready')

    result = ack_message(db_path, message_id=sent['message_id'], agent='agent_b')

    assert result['ok'] is False
    assert result['status'] == 'NOT_FOUND_OR_NOT_ACK_PENDING'

    conn = sqlite3.connect(db_path)
    try:
        status = conn.execute('SELECT status FROM messages WHERE message_id = ?', (sent['message_id'],)).fetchone()[0]
        assert status == 'QUEUED'
    finally:
        conn.close()


def test_ack_rejects_wrong_agent(tmp_path):
    from anyue_bus_core import init_db, ack_message

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _ack_pending_message(db_path, content='ack-wrong-agent')

    result = ack_message(db_path, message_id=seed['message_id'], agent='agent_a')

    assert result['ok'] is False
    assert result['status'] == 'NOT_FOUND_OR_NOT_ACK_PENDING'

    conn = sqlite3.connect(db_path)
    try:
        status = conn.execute('SELECT status FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()[0]
        assert status == 'ACK_PENDING'
    finally:
        conn.close()


def test_explicit_echo_ack_becomes_fallback_received_not_acked(tmp_path):
    from anyue_bus_core import init_db, ack_message

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _ack_pending_message(db_path, content='explicit-echo')

    result = ack_message(db_path, message_id=seed['message_id'], agent='agent_b', reply='echo', processing_mode='explicit_echo')

    assert result['status'] == 'FALLBACK_RECEIVED'
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT status, processing_mode FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()
        assert row == ('FALLBACK_RECEIVED', 'explicit_echo')
    finally:
        conn.close()


def test_fallback_ack_requeues_message_for_retry(tmp_path):
    from anyue_bus_core import init_db, ack_message

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _ack_pending_message(db_path, content='fallback-retry')

    result = ack_message(db_path, message_id=seed['message_id'], agent='agent_b', reply='failed', processing_mode='fallback', error_detail='native timeout')

    assert result['status'] == 'QUEUED'
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT status, retry_count, error_detail FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()
        assert row == ('QUEUED', 1, 'native timeout')
    finally:
        conn.close()
