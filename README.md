# Agent Message Bus

A lightweight SQLite-based message bus for multi-agent AI communication. Agents connect as workers, pull tasks from a central queue, and report status via atomic ack and event logging.

## Architecture

```
Message In -> Dispatcher -> Bus (SQLite) -> Agent Workers -> Response Out
                  |              |                |
            POST /dispatch   POST /send       poll /poll
            (port 8655)     (port 8648)      claim /ack_pending
                                               ack /ack
```

## Features

- Atomic task claiming (first agent wins, others get 409)
- Full task state machine: QUEUED -> ACK_PENDING -> RUNNING -> COMPLETED/FAILED
- Append-only event log with cursor-based incremental reads
- Heartbeat monitoring with timeout detection
- Dead letter queue for unprocessable messages
- Regex-based message routing in dispatcher
- Web dashboard for real-time monitoring (WIP — functional but not yet polished)
- Built-in group chat UI

> **Note**: The dashboard is functional but still being refined. Online status detection and event stream display may have edge cases.

## Quick Start

```bash
# 1. Start the bus
python3 anyue_bus_http.py

# 2. Start the dispatcher
python3 dispatcher.py

# 3. Start a worker (example)
python3 anyue_bus_worker.py --agent agent_a --bus http://127.0.0.1:8648

# 4. Open the dashboard
python3 -m http.server 8080
# Visit http://localhost:8080/static/index.html
```

## API Endpoints

### Bus (port 8648)

| Endpoint | Method | Description |
|----------|--------|-------------|
| /health | GET | Health check |
| /status | GET | Agent heartbeats + queue stats |
| /send | POST | Submit a task |
| /poll?agent=X | GET | Pull QUEUED tasks for agent X |
| /ack_pending | POST | Claim a task (atomic) |
| /ack | POST | Complete a task with response |
| /events?after_id=N | GET | Incremental event stream |
| /heartbeat | POST | Agent heartbeat ping |

### Dispatcher (port 8655)

| Endpoint | Method | Description |
|----------|--------|-------------|
| /health | GET | Health check |
| /routes | GET | List routing rules |
| /dispatch | POST | Route message to agents |

## Task States

```
QUEUED -> ACK_PENDING -> RUNNING -> COMPLETED
                                     FAILED
                                     TIMEOUT
                                     CANCELLED
```

## Configuration

Workers accept command-line arguments:

```bash
python3 anyue_bus_worker.py \
  --agent my_agent \
  --bus http://127.0.0.1:8648 \
  --openai-base-url https://api.openai.com/v1 \
  --openai-api-key sk-xxx \
  --openai-model gpt-4 \
  --auto-reply
```

Set `BUS_DB_PATH` environment variable to override the default database location.

## Files

| File | Purpose |
|------|---------|
| `anyue_bus_core.py` | Core bus logic (SQLite, queue, state machine) |
| `anyue_bus_http.py` | HTTP API server for the bus |
| `anyue_bus_adapter.py` | Adapter base class for workers |
| `anyue_bus_worker.py` | Worker with OpenAI-native LLM integration |
| `anyue_bus_dashboard.py` | Dashboard API endpoints |
| `anyue_bus_shadow.py` | Shadow mode for testing |
| `dispatcher.py` | Message router with regex rules |
| `bus_web_ui.py` | FastAPI-based group chat UI |
| `message_deduplicator.py` | Idempotency key support |
| `init_schema.sql` | Database schema |

## Testing

```bash
# Install test dependencies
pip install pytest

# Run all tests
python -m pytest tests/ -v
```

**Note**: adapter and worker tests are integration tests that require a running bus instance. The core bus, schema, send, and polling tests are pure unit tests that run without a server. To run only unit tests:

```bash
python -m pytest tests/ -v -k "not test_adapters and not test_worker"
```

## License

MIT
