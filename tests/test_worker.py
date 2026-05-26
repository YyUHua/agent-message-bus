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


class FakeNativeServer:
    def __init__(self, reply_text='native reply'):
        self.reply_text = reply_text
        self.server = None
        self.thread = None
        self.url = None
        self.requests = []

    def __enter__(self):
        outer = self

        class Handler(__import__('http.server').server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get('Content-Length', '0') or '0')
                payload = json.loads(self.rfile.read(length).decode('utf-8'))
                outer.requests.append(payload)
                body = json.dumps({
                    'choices': [{'message': {'content': outer.reply_text}}]
                }).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return

        self.server = __import__('http.server').server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        host, port = self.server.server_address
        self.url = f'http://{host}:{port}/v1/chat/completions'
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def test_worker_calls_native_model_acks_and_sends_reply_to_peer(tmp_path):
    from anyue_bus_adapter import AnyueBusAdapter
    from anyue_bus_core import init_db
    from anyue_bus_worker import run_worker

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with BusServer(db_path) as bus, FakeNativeServer('AI hello from Nadia') as native:
        agent_a = AnyueBusAdapter('agent_a', bus.base_url)
        sent = agent_a.send('agent_b', 'please think and reply', client_msg_id='worker-native-1')

        run_worker('agent_b', bus.base_url, native.url, interval=0.01, once=True, auto_reply=True)

        status = agent_a.status()
        assert status['message_counts']['ACKED'] == 1
        replies = agent_a.poll(limit=1)
        assert replies[0]['from'] == 'agent_b'
        assert replies[0]['content'] == 'AI hello from Nadia'
        assert replies[0]['metadata']['auto_reply'] is True
        assert replies[0]['metadata']['in_reply_to'] == sent['message_id']
        assert native.requests
        assert native.requests[0]['messages'][-1]['content'] == 'please think and reply'


def test_worker_does_not_auto_reply_to_auto_reply_messages_to_prevent_ping_pong(tmp_path):
    from anyue_bus_adapter import AnyueBusAdapter
    from anyue_bus_core import init_db
    from anyue_bus_worker import run_worker

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with BusServer(db_path) as bus, FakeNativeServer('loop reply') as native:
        agent_b = AnyueBusAdapter('agent_b', bus.base_url)
        agent_b.send('agent_a', 'auto generated reply', client_msg_id='worker-loop-1', metadata={'auto_reply': True})

        run_worker('agent_a', bus.base_url, native.url, interval=0.01, once=True, auto_reply=True)

        status = agent_b.status()
        assert status['message_counts']['ACKED'] == 1
        next_msgs = agent_b.poll(limit=1)
        assert next_msgs == []


def test_worker_can_use_openai_style_native_endpoint(tmp_path):
    from anyue_bus_adapter import AnyueBusAdapter
    from anyue_bus_core import init_db
    from anyue_bus_worker import run_worker

    db_path = tmp_path / 'anyue_bus.db'
    init_db(db_path)

    with BusServer(db_path) as bus, FakeNativeServer('OpenAI-style reply') as native:
        agent_a = AnyueBusAdapter('agent_a', bus.base_url)
        sent = agent_a.send('agent_b', 'use openai style', client_msg_id='worker-openai-1')

        run_worker(
            'agent_b',
            bus.base_url,
            '',
            interval=0.01,
            once=True,
            auto_reply=True,
            openai_base_url=native.url.rsplit('/v1/chat/completions', 1)[0],
            openai_api_key='test-key',
            openai_model='deepseek/deepseek-chat',
        )

        replies = agent_a.poll(limit=1)
        assert replies[0]['content'] == 'OpenAI-style reply'
        assert replies[0]['metadata']['in_reply_to'] == sent['message_id']
        assert native.requests[0]['model'] == 'deepseek/deepseek-chat'
