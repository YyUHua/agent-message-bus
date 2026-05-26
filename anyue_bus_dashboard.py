#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.request


def get_json(base_url, path):
    with urllib.request.urlopen(base_url.rstrip('/') + path, timeout=5) as resp:
        return json.loads(resp.read().decode('utf-8'))


def short(text, n=90):
    text = (text or '').replace('\n', ' ')
    return text if len(text) <= n else text[:n] + '...'


def render(data, base_url):
    status = data['status']
    print('Agent Message Bus Dashboard')
    print('==================')
    print('bus:', base_url)
    print('message_counts:', status.get('message_counts', {}))
    print()
    print('agents:')
    agents = status.get('agents', [])
    if not agents:
        print('- none')
    for agent in agents:
        meta = agent.get('metadata_json') or '{}'
        print(f"- {agent['agent_id']}: {agent['status']} native={agent['native_status']} heartbeat={agent['last_heartbeat']} meta={short(meta, 70)}")
    print()
    print('processing:')
    processing = data.get('processing_messages') or []
    if not processing:
        print('- none')
    for msg in processing:
        print(f"- {msg['status']} {msg['message_id'][:8]} {msg['from_agent']} -> {msg['to_agent']}: {short(msg['content'])}")
    print()
    print('stuck:')
    stuck = status.get('stuck_messages') or []
    if not stuck:
        print('- none')
    for msg in stuck:
        print(f"- {msg['status']} {msg['message_id'][:8]} {msg['from_agent']} -> {msg['to_agent']}: {short(msg['content'])}")
        print(f"  retry={msg.get('retry_count')} max={msg.get('max_retries')} mode={msg.get('processing_mode')} detail={short(msg.get('error_detail'), 50)}")
    print()
    print('recent:')
    for msg in data.get('recent_messages', [])[:8]:
        reply = short(msg.get('reply'), 70)
        print(f"- {msg['status']} {msg['message_id'][:8]} {msg['from_agent']} -> {msg['to_agent']}: {short(msg['content'], 70)}")
        if reply:
            print(f"  reply: {reply}")


def main():
    parser = argparse.ArgumentParser(description='Render Anyue Bus dashboard from /v1/dashboard')
    parser.add_argument('base_url', nargs='?', default='http://127.0.0.1:8648')
    parser.add_argument('--json', action='store_true', help='print raw JSON')
    args = parser.parse_args()
    try:
        data = get_json(args.base_url, '/v1/dashboard')
    except Exception as exc:
        print(f'Cannot connect to Agent Message Bus: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        render(data, args.base_url.rstrip('/'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
