import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_shadow_mirror_legacy_queue_file_writes_bus_without_deleting_source(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_shadow import mirror_legacy_queue_file

    db_path = tmp_path / 'anyue_bus.db'
    queue_file = tmp_path / 'from_agent_a_001.json'
    init_db(db_path)
    queue_file.write_text(json.dumps({
        'from': 'agent_a',
        'to': 'agent_b',
        'message': 'shadow hello',
        'timestamp': 1778062600000,
    }, ensure_ascii=False), encoding='utf-8')

    result = mirror_legacy_queue_file(db_path, queue_file)

    assert result['ok'] is True
    assert result['status'] == 'QUEUED'
    assert queue_file.exists()

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT from_agent, to_agent, content, status, metadata_json FROM messages WHERE message_id = ?', (result['message_id'],)).fetchone()
        assert row[0] == 'agent_a'
        assert row[1] == 'agent_b'
        assert row[2] == 'shadow hello'
        assert row[3] == 'QUEUED'
        metadata = json.loads(row[4])
        assert metadata['shadow_mode'] is True
        assert metadata['legacy_path'] == str(queue_file)
    finally:
        conn.close()


def test_shadow_mirror_is_idempotent_for_same_legacy_file(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_shadow import mirror_legacy_queue_file

    db_path = tmp_path / 'anyue_bus.db'
    queue_file = tmp_path / 'from_agent_b_001.json'
    init_db(db_path)
    queue_file.write_text(json.dumps({'from': 'agent_b', 'to': 'agent_a', 'message': 'once'}, ensure_ascii=False), encoding='utf-8')

    first = mirror_legacy_queue_file(db_path, queue_file)
    second = mirror_legacy_queue_file(db_path, queue_file)

    assert second['ok'] is True
    assert second['message_id'] == first['message_id']
    assert second['status'] == 'DUPLICATE'

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        assert count == 1
    finally:
        conn.close()


def test_shadow_scan_directory_mirrors_only_json_files(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_shadow import scan_legacy_queue_dir

    db_path = tmp_path / 'anyue_bus.db'
    queue_dir = tmp_path / 'queue'
    queue_dir.mkdir()
    init_db(db_path)
    (queue_dir / 'from_agent_a_1.json').write_text(json.dumps({'from': 'agent_a', 'to': 'agent_b', 'message': 'one'}, ensure_ascii=False), encoding='utf-8')
    (queue_dir / 'notes.txt').write_text('ignore', encoding='utf-8')

    result = scan_legacy_queue_dir(db_path, queue_dir)

    assert result['ok'] is True
    assert result['mirrored'] == 1
    assert result['skipped'] == 0


def test_shadow_mirror_rejects_invalid_file_without_bus_write(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_shadow import mirror_legacy_queue_file

    db_path = tmp_path / 'anyue_bus.db'
    queue_file = tmp_path / 'bad.json'
    init_db(db_path)
    queue_file.write_text('{bad json', encoding='utf-8')

    result = mirror_legacy_queue_file(db_path, queue_file)

    assert result['ok'] is False
    assert result['status'] == 'INVALID_LEGACY_FILE'

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        assert count == 0
    finally:
        conn.close()
