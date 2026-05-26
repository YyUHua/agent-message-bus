#!/usr/bin/env python3
"""Agent Message Bus — Multi-Agent Group Chat"""
import json, time, sqlite3, urllib.request, urllib.parse, subprocess, os, re, struct, math
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn

BUS_URL = "http://127.0.0.1:8648"
BUS_DB = Path(os.environ.get("BUS_DB_PATH", Path(__file__).parent / "anyue_bus.db"))
STATIC_DIR = Path(__file__).parent / "static"
PORT = 5200

app = FastAPI(title="Agent Message Bus — Group Chat")

# 静态文件
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# 禁用 API 路由的浏览器缓存
class NoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response
app.add_middleware(NoCacheMiddleware)

# ── 人物 ──
PEOPLE = {
    "user":  {"name": "User",   "color": "#07c160", "emoji": "🐑"},
    "agent_a":    {"name": "Agent A", "color": "#667eea", "emoji": "🌙"},
    "agent_b":     {"name": "Agent B", "color": "#f5576c", "emoji": "🌸"},
    "agent_c":    {"name": "Agent C", "color": "#f39c12", "emoji": "🔥"},
}
SEND_KEYS = {"agent_a": "agent_a", "agent_b": "agent_b", "agent_c": "agent_c"}


def bus_db():
    conn = sqlite3.connect(str(BUS_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=3000")
    return conn


def bus_post(path, data):
    body = json.dumps(data, ensure_ascii=False).encode()
    req = urllib.request.Request(f"{BUS_URL}{path}", data=body,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


# ── API ──

@app.get("/", response_class=HTMLResponse)
async def index():
    html = STATIC_DIR / "index.html"
    return html.read_text(encoding="utf-8") if html.exists() else "No index.html"


@app.get("/api/messages")
async def get_messages(limit: int = 100):
    """从总线DB读取所有对话消息，按时间排序"""
    try:
        conn = bus_db()
        cur = conn.execute("""
            SELECT * FROM (
                SELECT message_id, from_agent, to_agent, content, reply, status, created_at
                FROM messages
                WHERE from_agent NOT IN ('bus_core')
                ORDER BY created_at DESC
                LIMIT ?
            ) ORDER BY created_at ASC
        """, (limit,))
        messages = []
        for r in cur.fetchall():
            msg = dict(r)
            msg["created_at"] = msg["created_at"] // 1000  # ms→s
            messages.append(msg)
        conn.close()
        return {"messages": messages}
    except Exception as e:
        return {"error": str(e), "messages": []}


@app.post("/api/send")
async def send_message(req: Request):
    """发消息到总线"""
    data = await req.json()
    to_agent = data.get("to", "")
    content = data.get("content", "").strip()
    if not to_agent or not content:
        return JSONResponse({"error": "缺少收件人或消息"}, status_code=400)

    if to_agent == "all":
        results = {}
        for agent in ["agent_a", "agent_b", "agent_c"]:
            r = bus_post("/send", {
                "from": "user",
                "to": agent,
                "type": "chat",
                "content": content,
                "metadata": {"channel": "web_ui", "sender": "user"}
            })
            results[agent] = r
        return {"ok": True, "results": results}
    else:
        r = bus_post("/send", {
            "from": "user",
            "to": to_agent,
            "type": "chat",
            "content": content,
            "metadata": {"channel": "web_ui", "sender": "user"}
        })
        return {"ok": True, "result": r} if "error" not in r else {"error": r["error"]}


@app.get("/api/agents")
async def get_agents():
    """获取在线Agent状态"""
    try:
        conn = bus_db()
        cur = conn.execute("SELECT agent_id, status, native_status, last_heartbeat FROM agents")
        agents = {r["agent_id"]: dict(r) for r in cur.fetchall()}
        conn.close()
    except:
        agents = {}
    # 补充不在DB里的人物
    result = {}
    for key, info in PEOPLE.items():
        if key == "user":
            result[key] = {"name": info["name"], "status": "online", "native_status": "ok"}
        elif key in agents:
            result[key] = {**agents[key], "name": info["name"]}
        else:
            result[key] = {"name": info["name"], "status": "unknown", "native_status": "unknown"}
    return {"agents": result}


@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


# ── 图表 K 线数据 ──

def _ema(data, period):
    n=len(data)
    if n<period: return [None]*n
    r=[None]*n
    clean=[x for x in data if x is not None]
    if len(clean)<period: return [None]*n
    s=sum(clean[:period])/period
    r[period-1]=s
    a=2/(period+1)
    for i in range(period,n):
        val=data[i] if data[i] is not None else r[i-1]
        r[i]=(val-r[i-1])*a+r[i-1]
    return r

def _curl_binance_klines(sym, tf, limit=500):
    url=f"https://api.binance.com/api/v3/klines?symbol={sym.upper()}&interval={tf}&limit={limit}"
    r=subprocess.run(["curl","-s","--max-time","10",url],capture_output=True,timeout=15)
    if not r.stdout.strip(): return []
    try: d=json.loads(r.stdout)
    except: return []
    if not isinstance(d,list): return []
    return [[int(b[0])//1000,float(b[1]),float(b[2]),float(b[3]),float(b[4]),float(b[5])] for b in d]

@app.get("/api/chart/data")
async def chart_data(symbol:str="btcusdt", tf:str="4h", limit:int=300):
    bars=_curl_binance_klines(symbol,tf,limit)
    if not bars:
        return {"error":"no data"}
    close=[b[4] for b in bars]
    high=[b[3] for b in bars]
    low=[b[2] for b in bars]
    opn=[b[1] for b in bars]
    vol=[b[5] for b in bars]
    ts=[b[0] for b in bars]
    n=len(close)

    # MACD
    dif=_ema(close,12); dea=_ema(dif,9) if dif[-1] is not None else [None]*n
    bar=[(dif[i]-dea[i]) if dif[i] is not None and dea[i] is not None else None for i in range(n)]

    # 摆动高低点
    sl=5
    swing_low_idx=[i for i in range(sl,n-sl) if all(low[i]<=low[i-j] and low[i]<=low[i+j] for j in range(1,sl+1))]
    swing_high_idx=[i for i in range(sl,n-sl) if all(high[i]>=high[i-j] and high[i]>=high[i+j] for j in range(1,sl+1))]

    # 二买/二卖信号
    sig_long=[]; sig_short=[]
    for i in range(20,n):
        if bar[i] is None or bar[i]<=0: continue
        lows=[l for l in swing_low_idx if l<=i and l>i-40]
        if len(lows)<2: continue
        last,prev=lows[-1],lows[-2]
        if i-last>6: continue
        if close[last]<=close[prev]*1.001: continue
        if dif[last] is None or dif[prev] is None or dif[last]<dif[prev]-5: continue
        if not (close[i]>opn[i]): continue
        sig_long.append({"idx":last,"price":close[last],"sl":min(low[max(0,last-2):last+1])*0.997})
        # mark entry bar
        sig_long.append({"idx":i,"price":close[i],"entry":True})

    for i in range(20,n):
        if dif[i] is None: continue
        highs=[h for h in swing_high_idx if h<=i and h>i-40]
        if len(highs)<2: continue
        last,prev=highs[-1],highs[-2]
        if i-last>6: continue
        if close[last]>=close[prev]*0.999: continue
        if dif[last] is None or dif[prev] is None or dif[last]>dif[prev]+5: continue
        if not (close[i]<opn[i]): continue
        sig_short.append({"idx":last,"price":close[last],"sl":max(high[max(0,last-2):last+1])*1.003})
        sig_short.append({"idx":i,"price":close[i],"entry":True})

    # 季节
    dt=dif
    if n>30 and dt[-1] is not None:
        slp10=dt[-1]-(dt[-10] if dt[-10] is not None else dt[-1])
        slp5=dt[-1]-(dt[-5] if dt[-5] is not None else dt[-1])
        is_w=abs(dt[-1])<15
        is_sp=dt[-1]>0 and (dt[-20] if dt[-20] is not None else 0)<5 and dt[-1]>5 and slp10>0
        is_sm=dt[-1]>0 and slp10>0
        is_a=dt[-1]>60 and slp10<=-1
        eb=dt[-1]>80 and slp5>3
        ew=dt[-1]<-80 and slp5<-3
        if eb: season,mode="极端多头","bull_extreme"
        elif ew: season,mode="极端空头","bear_extreme"
        elif is_w: season,mode="冬","normal"
        elif is_sp: season,mode="春","normal"
        elif is_a: season,mode="秋","normal"
        else: season,mode="夏","normal"
        dir_f="仅做多" if (eb or dt[-1]>0) else "仅做空"
    else:
        season,mode,dir_f="夏","normal","仅做多"

    return {
        "klines":[{"t":ts[i],"o":opn[i],"h":high[i],"l":low[i],"c":close[i],"v":vol[i]} for i in range(n)],
        "dif":[round(d,2) if d is not None else None for d in dif],
        "dea":[round(d,2) if d is not None else None for d in dea],
        "bar":[round(b,2) if b is not None else None for b in bar],
        "swing_lows":swing_low_idx,
        "swing_highs":swing_high_idx,
        "signals_long":sig_long,
        "signals_short":sig_short,
        "season":season,
        "mode":mode,
        "dir_filter":dir_f,
        "dif_val":round(dt[-1],1) if dt and dt[-1] is not None else 0,
    }

@app.get("/chart", response_class=HTMLResponse)
async def chart_page():
    html=STATIC_DIR/"chart.html"
    return html.read_text(encoding="utf-8") if html.exists() else HTMLResponse("No chart.html",status_code=404)


# ── 交易引擎代理 ──
TRADER_URL = "http://127.0.0.1:8000"

@app.get("/api/stats")
async def get_stats():
    """系统资源 + token 估算"""
    # CPU —— 所有进程 %cpu 求和
    try:
        r = subprocess.run(['ps', '-A', '-o', '%cpu'], capture_output=True, text=True, timeout=3)
        cpu_total = sum(float(l) for l in r.stdout.strip().split('\n')[1:] if l.strip())
        cpu_count = os.cpu_count() or 1
        cpu_pct = round(min(cpu_total / cpu_count, 100), 1)
    except Exception:
        cpu_pct = 0

    # 内存 —— vm_stat + sysctl
    try:
        r = subprocess.run(['sysctl', '-n', 'hw.memsize'], capture_output=True, text=True, timeout=3)
        mem_total = int(r.stdout.strip())
        r2 = subprocess.run(['vm_stat'], capture_output=True, text=True, timeout=3)
        page_size = 16384  # macOS 默认 page size
        for line in r2.stdout.split('\n'):
            # page size may vary, extract it
            m = re.search(r'page size of (\d+) bytes', line)
            if m:
                page_size = int(m.group(1))
            # active + wired = in use
        active = wired = 0
        for line in r2.stdout.split('\n'):
            m = re.match(r'Pages active:\s+(\d+)', line)
            if m: active = int(m.group(1))
            m = re.match(r'Pages wired down:\s+(\d+)', line)
            if m: wired = int(m.group(1))
        mem_used = (active + wired) * page_size
        mem_pct = round(mem_used / mem_total * 100, 1) if mem_total else 0
    except Exception:
        mem_total = 0
        mem_used = 0
        mem_pct = 0

    # Tokens 估算 —— 从消息表统计
    input_chars = output_chars = 0
    msg_count = 0
    try:
        conn = bus_db()
        rows = conn.execute(
            "SELECT from_agent, length(content) as clen FROM messages WHERE from_agent NOT IN ('bus_core')"
        ).fetchall()
        conn.close()
        for r in rows:
            msg_count += 1
            if r['from_agent'] == 'user':
                input_chars += r['clen']
            else:
                output_chars += r['clen']
    except Exception:
        pass

    # 中文约 2 chars/token, 英文约 4 chars/token，简化按 2.5 算
    return {
        "cpu": cpu_pct,
        "memory": {
            "total": mem_total,
            "used": mem_used,
            "percent": mem_pct,
        },
        "tokens": {
            "input": round(input_chars / 2.5),
            "output": round(output_chars / 2.5),
            "total": round((input_chars + output_chars) / 2.5),
        },
        "messages": msg_count,
    }


# ── 启动 ──
if __name__ == "__main__":
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Agent Message Bus -> http://localhost:{PORT}")
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="info")
