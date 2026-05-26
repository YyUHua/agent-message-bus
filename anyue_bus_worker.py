import json
import sqlite3
import time
import os
import argparse
import urllib.request
from pathlib import Path
from datetime import datetime

from anyue_bus_adapter import AnyueBusAdapter

PERSONA_PROMPTS = {
    'agent_a': (
        'You are Agent A, a helpful AI assistant. '
        'Respond concisely in the target language. Be natural, not robotic.'
    ),
    'agent_b': (
        'You are Agent B, a supportive AI assistant. '
        'Respond concisely in the target language. Be warm and direct.'
    ),
}


def build_prompt(agent, msg):
    meta = msg.get('metadata') or {}
    history = meta.get('conversation_history') or []
    peer = msg.get('from', 'peer')
    persona = PERSONA_PROMPTS.get(agent, f'你是{agent}')
    system = (
        f'{persona} You are talking to {peer} via Agent Message Bus. '
        '请优先回应对方这一次说的话，并尽量自然延续上下文。'
        '如果有历史消息，请把它们当作最近对话背景。'
        '不要输出项目符号，不要提系统，不要说“作为AI”。'
    )
    messages = [{'role': 'system', 'content': system}]
    for item in history[-6:]:
        role = item.get('role', 'user')
        content = item.get('content', '')
        if role in ('user', 'assistant') and content:
            messages.append({'role': role, 'content': content})
    messages.append({'role': 'user', 'content': msg.get('content', '')})
    return messages


def call_native(native_url, agent, incoming):
    payload = {
        'messages': build_prompt(agent, incoming),
        'max_tokens': 300,
    }
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(native_url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode('utf-8'))
    return result['choices'][0]['message']['content'].strip()


def call_openai_native(base_url, api_key, model, agent, incoming):
    payload = {
        'model': model,
        'messages': build_prompt(agent, incoming),
        'max_tokens': 300,
    }
    url = base_url.rstrip('/') + '/chat/completions'
    req = urllib.request.Request(url, data=json.dumps(payload, ensure_ascii=False).encode('utf-8'), method='POST')
    req.add_header('Content-Type', 'application/json')
    if api_key:
        req.add_header('Authorization', 'Bearer ' + api_key)
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode('utf-8'))
    return result['choices'][0]['message']['content'].strip()


def fallback_reply(agent, incoming, error):
    return f"{agent} worker received message but native model failed: {type(error).__name__}: {error}"


def should_send_auto_reply(agent, msg):
    metadata = msg.get('metadata') or {}
    return (
        msg.get('from') in ('agent_a', 'agent_b')
        and msg.get('from') != agent
        and not metadata.get('auto_reply')
    )


def generate_reply(agent, msg, native_url, openai_base_url, openai_api_key, openai_model, auto_reply):
    if not auto_reply:
        return f"{agent} received: {msg.get('content')}"
    if native_url:
        return call_native(native_url, agent, msg)
    if openai_base_url and openai_model:
        return call_openai_native(openai_base_url, openai_api_key, openai_model, agent, msg)
    return f"{agent} received: {msg.get('content')}"


def _ack_with_policy(adapter, mid, agent, reply, mode, error_detail=''):
    if mode == 'ai':
        return adapter.ack(mid, reply=reply)
    if mode == 'explicit_echo':
        return adapter.ack(mid, reply=reply, processing_mode='explicit_echo', error_detail=error_detail)
    return adapter.ack(mid, reply=reply, processing_mode='fallback', error_detail=error_detail)


def run_worker(agent, bus_url, native_url, interval, once=False, auto_reply=False, openai_base_url='', openai_api_key='', openai_model=''):
    adapter = AnyueBusAdapter(agent=agent, base_url=bus_url)
    native_label = native_url or (openai_base_url.rstrip('/') + '/chat/completions' if openai_base_url else '')
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {agent} worker online bus={bus_url} native={native_label} auto_reply={auto_reply}", flush=True)
    while True:
        try:
            adapter.heartbeat(status='online', native_status='configured' if native_label else 'none', native_url=native_label, metadata={'worker': True, 'auto_reply': auto_reply})
            messages = adapter.poll(limit=5)
            for msg in messages:
                mid = msg['message_id']
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {agent} got {mid} from {msg.get('from')}: {msg.get('content')}", flush=True)
                adapter.ack_pending(mid)
                error_detail = ''
                try:
                    reply = generate_reply(agent, msg, native_url, openai_base_url, openai_api_key, openai_model, auto_reply)
                    mode = 'ai' if auto_reply and (native_url or openai_base_url) else 'explicit_echo'
                except Exception as exc:
                    reply = fallback_reply(agent, msg, exc)
                    mode = 'fallback'
                    error_detail = f"{type(exc).__name__}: {exc}"
                ack_result = _ack_with_policy(adapter, mid, agent, reply, mode, error_detail=error_detail)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {agent} acked {mid} mode={mode} status={ack_result.get('status')}: {reply[:160]}", flush=True)
                if auto_reply and mode == 'ai' and ack_result.get('status') == 'ACKED' and should_send_auto_reply(agent, msg):
                    next_history = list((msg.get('metadata') or {}).get('conversation_history') or [])
                    next_history.append({'role': 'user', 'content': msg.get('content', '')})
                    next_history.append({'role': 'assistant', 'content': reply})
                    adapter.send(
                        to_agent=msg['from'],
                        content=reply,
                        metadata={
                            'auto_reply': True,
                            'in_reply_to': mid,
                            'processing_mode': mode,
                            'conversation_history': next_history[-6:],
                        },
                    )
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {agent} sent reply to {msg['from']}", flush=True)
        except Exception as exc:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {agent} worker error: {type(exc).__name__}: {exc}", flush=True)
        if once:
            break
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description='Anyue Bus polling worker')
    parser.add_argument('--agent', required=True, choices=['agent_a', 'agent_b'])
    parser.add_argument('--bus-url', default='http://127.0.0.1:8648')
    parser.add_argument('--native-url', default='')
    parser.add_argument('--openai-base-url', default=os.environ.get('ANYUE_OPENAI_BASE_URL', ''))
    parser.add_argument('--openai-api-key', default=os.environ.get('ANYUE_OPENAI_API_KEY', ''))
    parser.add_argument('--openai-model', default=os.environ.get('ANYUE_OPENAI_MODEL', ''))
    parser.add_argument('--interval', type=float, default=1.0)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--auto-reply', action='store_true')
    args = parser.parse_args()
    run_worker(
        args.agent,
        args.bus_url.rstrip('/'),
        args.native_url,
        args.interval,
        once=args.once,
        auto_reply=args.auto_reply,
        openai_base_url=args.openai_base_url,
        openai_api_key=args.openai_api_key,
        openai_model=args.openai_model,
    )


if __name__ == '__main__':
    main()
