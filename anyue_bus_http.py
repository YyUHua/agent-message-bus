from pathlib import Path
from typing import Optional, Union
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from anyue_bus_core import (
    ack_message,
    ack_pending,
    clear_control_signal,
    create_control_signal,
    get_dashboard,
    get_health,
    get_status,
    init_db,
    list_dead_letters,
    move_to_dead_letter,
    poll_messages,
    record_heartbeat,
    requeue_stale_messages,
    send_message,
)


class AnyueBusHandler(BaseHTTPRequestHandler):
    db_path: Optional[Path] = None

    def _write_json(self, status_code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get('Content-Length', '0') or '0')
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode('utf-8'))

    def log_message(self, format, *args):
        return

    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/v1/health':
            self._write_json(200, get_health(self.db_path))
            return
        if path == '/v1/status':
            self._write_json(200, get_status(self.db_path))
            return
        if path == '/v1/dashboard':
            self._write_json(200, get_dashboard(self.db_path))
            return
        if path == '/v1/dead':
            self._write_json(200, list_dead_letters(self.db_path))
            return
        self._write_json(404, {'ok': False, 'error': 'NOT_FOUND'})

    def do_POST(self):
        path = urlparse(self.path).path
        data = self._read_json()
        if path == '/v1/send':
            result = send_message(
                self.db_path,
                from_agent=data['from_agent'],
                to_agent=data['to_agent'],
                content=data['content'],
                type=data.get('type', 'chat'),
                client_msg_id=data.get('client_msg_id'),
                priority=data.get('priority', 5),
                metadata=data.get('metadata'),
            )
            self._write_json(200, result)
            return
        if path == '/v1/poll':
            result = poll_messages(self.db_path, agent=data['agent'], limit=data.get('limit', 1))
            self._write_json(200, result)
            return
        if path == '/v1/ack_pending':
            result = ack_pending(self.db_path, message_id=data['message_id'], agent=data['agent'])
            self._write_json(200, result)
            return
        if path == '/v1/ack':
            result = ack_message(
                self.db_path,
                message_id=data['message_id'],
                agent=data['agent'],
                reply=data.get('reply'),
                processing_mode=data.get('processing_mode'),
                error_detail=data.get('error_detail'),
            )
            self._write_json(200, result)
            return
        if path == '/v1/requeue_stale':
            result = requeue_stale_messages(self.db_path, stale_ms=data.get('stale_ms', 120000))
            self._write_json(200, result)
            return
        if path == '/v1/control':
            action = data.get('action', 'create')
            if action == 'create':
                result = create_control_signal(self.db_path, command=data['command'], requested_by=data['requested_by'], reason=data.get('reason'))
                self._write_json(200, result)
                return
            if action == 'clear':
                result = clear_control_signal(self.db_path, signal_id=data['signal_id'])
                self._write_json(200, result)
                return
        if path == '/v1/heartbeat':
            result = record_heartbeat(
                self.db_path,
                agent=data['agent'],
                adapter_url=data.get('adapter_url'),
                native_url=data.get('native_url'),
                status=data.get('status', 'unknown'),
                native_status=data.get('native_status', 'unknown'),
                metadata=data.get('metadata'),
            )
            self._write_json(200, result)
            return
        if path == '/v1/dead':
            result = move_to_dead_letter(self.db_path, message_id=data['message_id'], reason=data['reason'])
            self._write_json(200, result)
            return
        self._write_json(404, {'ok': False, 'error': 'NOT_FOUND'})


def create_app_server(host: str, port: int, db_path: Union[str, Path]):
    init_db(db_path)
    handler = type('AnyueBusHandlerBound', (AnyueBusHandler,), {'db_path': Path(db_path)})
    return ThreadingHTTPServer((host, port), handler)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Run Anyue Bus HTTP API')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8648)
    parser.add_argument('--db', default=str(Path(__file__).resolve().parent / 'anyue_bus.db'))
    args = parser.parse_args()
    server = create_app_server(args.host, args.port, args.db)
    print(f'Anyue Bus HTTP API listening on http://{args.host}:{args.port} db={args.db}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
