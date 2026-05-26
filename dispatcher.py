#!/usr/bin/env python3
"""Agent Message Bus Dispatcher — port 8655, receive messages -> regex route -> broadcast to matching agents"""
import json, re, uuid, time, logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse
from urllib.request import Request, urlopen

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dispatcher")

BUS_URL = "http://127.0.0.1:8648"
PORT = 8655

# ===== 路由规则 v1（[Router]交付 2026-05-26） =====
# 广播模式：一条消息命中多条规则时，所有命中 Agent 均收到任务。
# 无匹配 → 回群聊提示，不堆给[Router]。
ROUTES = [
    # 1. 代码/工程类
    (r"(写代码|代码|实现|功能|bug|修|部署|后端|接口|API|Python|Go|Node|Rust|React)",
     "code", ["agent_d"]),
    (r"(交易|策略|回测|OKX|Gate|K线|行情)",
     "trading", ["agent_a"]),
    (r"(排盘|八字|五行|卦|风水|图阿凸)",
     "fortune", ["agent_e"]),

    # 2. 设计/文案类
    (r"(设计|UI|前端|页面|布局|CSS|HTML)",
     "design", ["agent_a"]),
    (r"(文案|早安|公众号|SKU|标题|海报)",
     "copywriting", ["agent_e"]),
    (r"(小说|角色|人设|设定|世界书)",
     "writing", ["agent_e"]),

    # 3. 系统/架构类
    (r"(架构|体系|规划|方案|技术选型|协议)",
     "architecture", ["agent_c"]),
    (r"(面板|监控|状态|总线|部署)",
     "systems", ["agent_a"]),

    # 4. 协作/日常类
    (r"(你们四个|全体|大家一起|所有人)",
     "all_hands", ["agent_e", "agent_a", "agent_d", "agent_c"]),
    (r"(帮我|请教|问个问题|有个事)",
     "help", ["agent_e"]),
    (r"(早|晚安|吃了吗|下班|在吗)",
     "daily", ["agent_e"]),
    # 兜底 — 没命中任何规则的默认给[Router]
    (r".",
     "catch_all", ["agent_e"]),
]

def match_agents(content: str):
    """返回 (命中关键词, [agent列表])。重叠命中合并广播。"""
    hits = set()
    for pattern, keyword, agents in ROUTES:
        if re.search(pattern, content, re.IGNORECASE):
            hits.update(agents)
    return hits or None


def _json_post(url, payload, timeout=10):
    data = json.dumps(payload, ensure_ascii=False).encode()
    req = Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

class DispatchHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _parse_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def _reply(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/")
        if path == "" or path == "/health":
            self._reply({"ok": True, "service": "dispatcher", "port": PORT})
            return
        if path == "/routes":
            self._reply({"ok": True, "routes": [{"pattern": p, "keyword": k, "agents": a} for p, k, a in ROUTES]})
            return
        self._reply({"ok": False, "error": "not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")

        if path in ("/dispatch", ""):
            body = self._parse_body()
            content = body.get("content", "")
            from_user = body.get("from_user", "unknown")
            parent_task_id = body.get("parent_task_id", "")

            if not content:
                self._reply({"ok": False, "error": "missing content"}, 400)
                return

            agents = match_agents(content)
            if not agents:
                self._reply({
                    "ok": True,
                    "dispatched": False,
                    "hint": "没有匹配到对应 Agent，请在群聊中说明具体需求（代码/设计/文案/搜索/状态）",
                })
                return

            task_id = f"dispatcher-{int(time.time()*1000)}"
            dispatched = []
            for agent in agents:
                try:
                    result = _json_post(f"{BUS_URL}/v1/send", {
                        "from_agent": "user",
                        "to_agent": agent,
                        "content": content,
                        "type": "task",
                        "priority": 5,
                        "client_msg_id": task_id,
                        "metadata": {
                            "from_user": from_user,
                            "via": "dispatcher",
                            "parent_task_id": parent_task_id,
                            "visibility": "task",
                        },
                    })
                    dispatched.append({"agent": agent, "message_id": result.get("message_id", "?")})
                except Exception as e:
                    dispatched.append({"agent": agent, "error": str(e)})

            log.info(f"dispatch {task_id}: {content[:50]} → {','.join(agents)}")
            self._reply({
                "ok": True,
                "dispatched": True,
                "task_id": task_id,
                "agents": dispatched,
            })
            return

        self._reply({"ok": False, "error": "not found"}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


def run():
    server = ThreadedHTTPServer(("0.0.0.0", PORT), DispatchHandler)
    log.info(f"Dispatcher listening on :{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down")
        server.shutdown()


if __name__ == "__main__":
    run()
