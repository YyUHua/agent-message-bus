from anyue_bus_core import get_dashboard, get_health, get_status


def test_dashboard_includes_recent_messages_and_processing_state(tmp_path):
    from anyue_bus_core import init_db, send_message, record_heartbeat

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    record_heartbeat(db_path, agent='agent_a', status='online')
    sent = send_message(db_path, 'agent_a', 'agent_b', 'dashboard demo', client_msg_id='dashboard-1')

    payload = get_dashboard(db_path)
    assert payload['ok'] is True
    assert payload['recent_messages'][0]['message_id'] == sent['message_id']
    assert payload['recent_messages'][0]['content'] == 'dashboard demo'
    assert payload['status']['message_counts']['QUEUED'] == 1
    assert payload['status']['agents'][0]['agent_id'] == 'agent_a'
    assert 'stuck_messages' in payload['status']
