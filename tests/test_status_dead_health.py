import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_record_heartbeat_upserts_agent_status(tmp_path):
    from anyue_bus_core import init_db, record_heartbeat

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    result = record_heartbeat(
        db_path,
        agent='agent_a',
        adapter_url='http://localhost:8641',
        native_url='http://localhost:9090',
        status='online',
        native_status='online',
        metadata={'model': 'qwen'},
    )

    assert result == {'ok': True, 'agent': 'agent_a', 'status': 'online'}

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            'SELECT agent_id, adapter_url, native_url, status, native_status, last_heartbeat, metadata_json FROM agents WHERE agent_id = ?',
            ('agent_a',),
        ).fetchone()
        assert row[0] == 'agent_a'
        assert row[1] == 'http://localhost:8641'
        assert row[2] == 'http://localhost:9090'
        assert row[3] == 'online'
        assert row[4] == 'online'
        assert row[5] is not None
        assert 'qwen' in row[6]
    finally:
        conn.close()


def test_get_status_returns_agents_queue_counts_and_active_controls(tmp_path):
    from anyue_bus_core import init_db, send_message, poll_messages, create_control_signal, record_heartbeat, get_status

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    send_message(db_path, from_agent='agent_a', to_agent='agent_b', content='queued', client_msg_id='queued')
    send_message(db_path, from_agent='agent_a', to_agent='agent_b', content='dispatched', client_msg_id='dispatched')
    poll_messages(db_path, agent='agent_b', limit=1)
    create_control_signal(db_path, command='PAUSE', requested_by='boss')
    record_heartbeat(db_path, agent='agent_b', status='online')

    result = get_status(db_path)

    assert result['ok'] is True
    assert result['message_counts']['QUEUED'] == 1
    assert result['message_counts']['DISPATCHED'] == 1
    assert result['agents'][0]['agent_id'] == 'agent_b'
    assert result['agents'][0]['status'] == 'online'
    assert result['active_controls'][0]['command'] == 'PAUSE'


def test_health_reports_ok_after_database_init(tmp_path):
    from anyue_bus_core import init_db, get_health

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    result = get_health(db_path)

    assert result['ok'] is True
    assert result['database'] == 'ok'
    assert result['wal_enabled'] is True


def test_move_to_dead_letter_marks_message_failed_and_records_dead_letter(tmp_path):
    from anyue_bus_core import init_db, send_message, move_to_dead_letter

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    sent = send_message(db_path, from_agent='agent_a', to_agent='agent_b', content='坏消息', client_msg_id='bad')

    result = move_to_dead_letter(db_path, message_id=sent['message_id'], reason='超过最大重试')

    assert result == {'ok': True, 'message_id': sent['message_id'], 'status': 'FAILED'}

    conn = sqlite3.connect(db_path)
    try:
        msg = conn.execute('SELECT status, failed_at, error_detail FROM messages WHERE message_id = ?', (sent['message_id'],)).fetchone()
        assert msg[0] == 'FAILED'
        assert msg[1] is not None
        assert msg[2] == '超过最大重试'
        dead = conn.execute('SELECT message_id, final_status, reason FROM dead_letters WHERE message_id = ?', (sent['message_id'],)).fetchone()
        assert dead == (sent['message_id'], 'FAILED', '超过最大重试')
    finally:
        conn.close()


def test_list_dead_letters_returns_unacknowledged_dead_letters(tmp_path):
    from anyue_bus_core import init_db, send_message, move_to_dead_letter, list_dead_letters

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    sent = send_message(db_path, from_agent='agent_a', to_agent='agent_b', content='坏消息', client_msg_id='bad')
    move_to_dead_letter(db_path, message_id=sent['message_id'], reason='超过最大重试')

    result = list_dead_letters(db_path)

    assert result['ok'] is True
    assert result['dead_letters'][0]['message_id'] == sent['message_id']
    assert result['dead_letters'][0]['reason'] == '超过最大重试'
