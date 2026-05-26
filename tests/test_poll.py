import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _seed_message(db_path, from_agent='agent_a', to_agent='agent_b', content='待领取消息', priority=5):
    from anyue_bus_core import send_message

    return send_message(
        db_path,
        from_agent=from_agent,
        to_agent=to_agent,
        content=content,
        client_msg_id=f'seed-{content}',
        priority=priority,
        metadata={'trace_id': f'trace-{content}', 'is_internal': True},
    )


def test_poll_message_claims_queued_message_atomically(tmp_path):
    from anyue_bus_core import init_db, poll_messages

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    seed = _seed_message(db_path, content='poll-1', priority=9)

    result = poll_messages(db_path, agent='agent_b', limit=1)

    assert result['ok'] is True
    assert result['messages']
    assert result['messages'][0]['message_id'] == seed['message_id']
    assert result['messages'][0]['status'] == 'DISPATCHED'

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT status, dispatched_at FROM messages WHERE message_id = ?', (seed['message_id'],)).fetchone()
        assert row[0] == 'DISPATCHED'
        assert row[1] is not None
    finally:
        conn.close()


def test_poll_message_respects_priority_and_fifo_for_ties(tmp_path):
    from anyue_bus_core import init_db, poll_messages

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    first = _seed_message(db_path, content='poll-first', priority=5)
    second = _seed_message(db_path, content='poll-second', priority=9)
    third = _seed_message(db_path, content='poll-third', priority=9)

    result = poll_messages(db_path, agent='agent_b', limit=2)

    assert [m['message_id'] for m in result['messages']] == [second['message_id'], third['message_id']]
    assert result['messages'][0]['priority'] == 9
    assert result['messages'][1]['priority'] == 9
