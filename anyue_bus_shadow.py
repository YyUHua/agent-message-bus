from pathlib import Path
import json
import hashlib
from typing import Optional, Union

from anyue_bus_core import send_message


def _stable_client_msg_id(queue_file: Path) -> str:
    resolved = str(queue_file.resolve()) if queue_file.exists() else str(queue_file)
    digest = hashlib.sha256(resolved.encode('utf-8')).hexdigest()[:24]
    return 'shadow:' + digest


def _infer_agents(payload: dict, queue_file: Path):
    from_agent = payload.get('from_agent') or payload.get('from') or payload.get('sender')
    to_agent = payload.get('to_agent') or payload.get('to') or payload.get('receiver')

    name = queue_file.name.lower()
    if not from_agent:
        if 'from_agent_a' in name or 'to_agent_b' in name:
            from_agent = 'agent_a'
        elif 'from_agent_b' in name or 'to_agent_a' in name:
            from_agent = 'agent_b'
    if not to_agent:
        if from_agent == 'agent_a':
            to_agent = 'agent_b'
        elif from_agent == 'agent_b':
            to_agent = 'agent_a'

    return from_agent, to_agent


def mirror_legacy_queue_file(db_path: Union[str, Path], queue_file: Union[str, Path]):
    queue_file = Path(queue_file)
    try:
        payload = json.loads(queue_file.read_text(encoding='utf-8'))
    except Exception as exc:
        return {
            'ok': False,
            'status': 'INVALID_LEGACY_FILE',
            'error': str(exc),
            'legacy_path': str(queue_file),
        }

    if not isinstance(payload, dict):
        return {
            'ok': False,
            'status': 'INVALID_LEGACY_FILE',
            'error': 'legacy JSON root is not an object',
            'legacy_path': str(queue_file),
        }

    from_agent, to_agent = _infer_agents(payload, queue_file)
    content = payload.get('content') or payload.get('message') or payload.get('text')
    if not from_agent or not to_agent or content is None:
        return {
            'ok': False,
            'status': 'INVALID_LEGACY_FILE',
            'error': 'missing from/to/content',
            'legacy_path': str(queue_file),
        }

    metadata = dict(payload.get('metadata') or {})
    metadata.update({
        'shadow_mode': True,
        'legacy_path': str(queue_file),
        'legacy_payload': payload,
    })

    return send_message(
        db_path,
        from_agent,
        to_agent,
        str(content),
        type=payload.get('type', 'chat'),
        client_msg_id=payload.get('client_msg_id') or _stable_client_msg_id(queue_file),
        priority=int(payload.get('priority', 5)),
        metadata=metadata,
    )


def scan_legacy_queue_dir(db_path: Union[str, Path], queue_dir: Union[str, Path], *, glob: str = '*.json'):
    queue_dir = Path(queue_dir)
    mirrored = 0
    skipped = 0
    errors = []
    results = []
    for queue_file in sorted(queue_dir.glob(glob)):
        result = mirror_legacy_queue_file(db_path, queue_file)
        results.append(result)
        if result.get('ok'):
            if result.get('status') == 'QUEUED':
                mirrored += 1
        else:
            skipped += 1
            errors.append(result)
    return {
        'ok': True,
        'mirrored': mirrored,
        'skipped': skipped,
        'errors': errors,
        'results': results,
    }
