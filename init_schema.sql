CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    client_msg_id TEXT,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'chat',
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 5,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    processing_mode TEXT,
    processing_status TEXT,
    reply TEXT,
    error_code TEXT,
    error_detail TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    queued_at INTEGER,
    dispatched_at INTEGER,
    ack_pending_at INTEGER,
    acked_at INTEGER,
    failed_at INTEGER,
    UNIQUE(from_agent, client_msg_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_to_status
    ON messages(to_agent, status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_status
    ON messages(status);
CREATE INDEX IF NOT EXISTS idx_messages_created_at
    ON messages(created_at);
CREATE TABLE IF NOT EXISTS agents (
    agent_id TEXT PRIMARY KEY,
    adapter_url TEXT,
    native_url TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    native_status TEXT NOT NULL DEFAULT 'unknown',
    last_heartbeat INTEGER,
    stop_requested INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    message_id TEXT,
    event_type TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    detail TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS control_signals (
    signal_id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    reason TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    cleared_at INTEGER
);
CREATE TABLE IF NOT EXISTS dead_letters (
    message_id TEXT PRIMARY KEY,
    final_status TEXT NOT NULL,
    reason TEXT,
    created_at INTEGER NOT NULL,
    acknowledged_at INTEGER
);
