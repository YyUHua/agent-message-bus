import json
import threading
import time
import urllib.request
from pathlib import Path

from anyue_bus_http import create_app_server


class BusServer:
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
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def test_agent_a_adapter_can_send_and_agent_b_can_poll(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_adapter import AnyueBusAdapter

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with BusServer(db_path) as srv:
        agent_a = AnyueBusAdapter('agent_a', srv.base_url)
        agent_b = AnyueBusAdapter('agent_b', srv.base_url)

        send_payload = agent_a.send('agent_b', 'adapter hello', client_msg_id='adapter-send-1')
        assert send_payload['ok'] is True

        messages = agent_b.poll(limit=1)
        assert messages[0]['content'] == 'adapter hello'
        assert messages[0]['from'] == 'agent_a'
        assert messages[0]['status'] == 'DISPATCHED'


def test_agent_b_adapter_can_ack_cycle(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_adapter import AnyueBusAdapter

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with BusServer(db_path) as srv:
        agent_a = AnyueBusAdapter('agent_a', srv.base_url)
        agent_b = AnyueBusAdapter('agent_b', srv.base_url)

        sent = agent_a.send('agent_b', 'adapter ack', client_msg_id='adapter-ack-1')
        message_id = sent['message_id']
        polled = agent_b.poll(limit=1)
        assert polled[0]['message_id'] == message_id
        pending = agent_b.ack_pending(message_id)
        assert pending['status'] == 'ACK_PENDING'
        acked = agent_b.ack(message_id, reply='收到')
        assert acked['status'] == 'ACKED'


def test_adapter_heartbeat_visible_in_status(tmp_path):
    from anyue_bus_core import init_db
    from anyue_bus_adapter import AnyueBusAdapter

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with BusServer(db_path) as srv:
        agent_a = AnyueBusAdapter('agent_a', srv.base_url)
        agent_b = AnyueBusAdapter('agent_b', srv.base_url)
        agent_a.heartbeat(status='online')
        agent_b.heartbeat(status='online')

        status = agent_a.status()
        agents = {row['agent_id']: row['status'] for row in status['agents']}
        assert agents['agent_a'] == 'online'
        assert agents['agent_b'] == 'online'
