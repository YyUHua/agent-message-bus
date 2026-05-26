import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_create_control_signal_inserts_active_signal(tmp_path):
    from anyue_bus_core import init_db, create_control_signal

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    result = create_control_signal(db_path, command='PAUSE', requested_by='boss', reason='用户喊停')

    assert result['ok'] is True
    assert result['command'] == 'PAUSE'
    assert result['active'] is True
    assert result['signal_id']

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            'SELECT signal_id, command, requested_by, reason, active, created_at, cleared_at FROM control_signals WHERE signal_id = ?',
            (result['signal_id'],),
        ).fetchone()
        assert row[0] == result['signal_id']
        assert row[1] == 'PAUSE'
        assert row[2] == 'boss'
        assert row[3] == '用户喊停'
        assert row[4] == 1
        assert row[5] is not None
        assert row[6] is None
    finally:
        conn.close()


def test_get_active_control_signals_returns_only_active_signals(tmp_path):
    from anyue_bus_core import init_db, create_control_signal, clear_control_signal, get_active_control_signals

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    pause = create_control_signal(db_path, command='PAUSE', requested_by='boss')
    stop = create_control_signal(db_path, command='STOP', requested_by='boss', reason='紧急停止')
    clear_control_signal(db_path, signal_id=pause['signal_id'])

    result = get_active_control_signals(db_path)

    assert result['ok'] is True
    assert [s['signal_id'] for s in result['signals']] == [stop['signal_id']]
    assert result['signals'][0]['command'] == 'STOP'
    assert result['signals'][0]['reason'] == '紧急停止'


def test_clear_control_signal_marks_inactive(tmp_path):
    from anyue_bus_core import init_db, create_control_signal, clear_control_signal

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    signal = create_control_signal(db_path, command='PAUSE', requested_by='boss')

    result = clear_control_signal(db_path, signal_id=signal['signal_id'])

    assert result == {'ok': True, 'signal_id': signal['signal_id'], 'active': False}

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute('SELECT active, cleared_at FROM control_signals WHERE signal_id = ?', (signal['signal_id'],)).fetchone()
        assert row[0] == 0
        assert row[1] is not None
    finally:
        conn.close()


def test_clear_control_signal_reports_missing_signal_without_mutation(tmp_path):
    from anyue_bus_core import init_db, clear_control_signal

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    result = clear_control_signal(db_path, signal_id='missing')

    assert result == {'ok': False, 'signal_id': 'missing', 'status': 'NOT_FOUND_OR_ALREADY_CLEARED'}
