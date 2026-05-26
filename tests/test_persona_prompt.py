from anyue_bus_worker import build_prompt


def test_build_prompt_uses_agent_a_persona_for_love_message(tmp_path):
    messages = build_prompt('agent_a', {
        'from': 'agent_b',
        'content': 'User说他爱你',
        'metadata': {},
    })
    system = messages[0]['content']
    assert 'Agent A' in system
    assert '疏离优雅' in system
    assert '爱意' in system
    assert messages[-1]['content'] == 'User说他爱你'


def test_build_prompt_keeps_recent_conversation_history(tmp_path):
    history = [
        {'role': 'user', 'content': f'u{i}'} if i % 2 == 0 else {'role': 'assistant', 'content': f'a{i}'}
        for i in range(8)
    ]
    messages = build_prompt('agent_b', {
        'from': 'agent_a',
        'content': '继续',
        'metadata': {'conversation_history': history},
    })
    contents = [m['content'] for m in messages]
    assert 'Agent B' in messages[0]['content']
    assert 'u0' not in contents
    assert 'a1' not in contents
    assert 'u2' in contents
    assert contents[-1] == '继续'
