import json
import threading
import time
import urllib.request
from pathlib import Path

from anyue_bus_http import create_app_server


class ServerFixture:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.server = None
        self.thread = None
        self.base_url = None

    def __enter__(self):
        self.server = create_app_server('127.0.0.1', 0, self.db_path)
        host, port = self.server.server_address
        self.base_url = f'http://{host}:{port}'
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        for _ in range(50):
            try:
                with urllib.request.urlopen(self.base_url + '/v1/health', timeout=0.2) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.05)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.thread:
            self.thread.join(timeout=2)


def _request_json(method, url, payload=None):
    data = None if payload is None else json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req, timeout=2) as resp:
        body = resp.read().decode('utf-8')
        return resp.status, json.loads(body)


def test_http_send_and_poll_flow(tmp_path):
    from anyue_bus_core import init_db

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with ServerFixture(db_path) as srv:
        status, payload = _request_json(
            'POST',
            srv.base_url + '/v1/send',
            {
                'from_agent': 'agent_a',
                'to_agent': 'agent_b',
                'content': 'hello http',
                'client_msg_id': 'http-send-1',
            },
        )
        assert status == 200
        assert payload['ok'] is True
        assert payload['status'] == 'QUEUED'
        message_id = payload['message_id']

        status, payload = _request_json('POST', srv.base_url + '/v1/poll', {'agent': 'agent_b', 'limit': 1})
        assert status == 200
        assert payload['ok'] is True
        assert payload['messages'][0]['message_id'] == message_id
        assert payload['messages'][0]['status'] == 'DISPATCHED'


def test_http_ack_path_changes_message_state(tmp_path):
    from anyue_bus_core import init_db

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with ServerFixture(db_path) as srv:
        status, payload = _request_json(
            'POST',
            srv.base_url + '/v1/send',
            {
                'from_agent': 'agent_a',
                'to_agent': 'agent_b',
                'content': 'ack me',
                'client_msg_id': 'http-ack-1',
            },
        )
        message_id = payload['message_id']
        _request_json('POST', srv.base_url + '/v1/poll', {'agent': 'agent_b', 'limit': 1})
        _request_json('POST', srv.base_url + '/v1/ack_pending', {'message_id': message_id, 'agent': 'agent_b'})
        status, payload = _request_json('POST', srv.base_url + '/v1/ack', {'message_id': message_id, 'agent': 'agent_b', 'reply': '收到'})
        assert status == 200
        assert payload['ok'] is True
        assert payload['status'] == 'ACKED'


def test_http_health_and_status_are_servable(tmp_path):
    from anyue_bus_core import init_db, record_heartbeat

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    record_heartbeat(db_path, agent='agent_a', status='online')

    with ServerFixture(db_path) as srv:
        status, payload = _request_json('GET', srv.base_url + '/v1/health')
        assert status == 200
        assert payload['ok'] is True
        assert payload['database'] == 'ok'

        status, payload = _request_json('GET', srv.base_url + '/v1/status')
        assert status == 200
        assert payload['ok'] is True
        assert payload['agents'][0]['agent_id'] == 'agent_a'


def test_http_dashboard_surfaces_recent_messages_and_processing_state(tmp_path):
    from anyue_bus_core import init_db, send_message

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)
    sent = send_message(db_path, 'agent_a', 'agent_b', 'dashboard demo', client_msg_id='dashboard-1')

    with ServerFixture(db_path) as srv:
        status, payload = _request_json('GET', srv.base_url + '/v1/dashboard')
        assert status == 200
        assert payload['ok'] is True
        assert payload['recent_messages'][0]['message_id'] == sent['message_id']
        assert payload['recent_messages'][0]['content'] == 'dashboard demo'
        assert payload['status']['message_counts']['QUEUED'] == 1
