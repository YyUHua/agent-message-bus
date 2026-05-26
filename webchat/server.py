#!/usr/bin/env python3
"""WebChat thin client - routes messages to Hermes gateway (8642) with fixed session_id.
All channels sharing session_id='user' see the same conversation."""

import json, os, html
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import httpx, uvicorn

HOST = "127.0.0.1"
PORT = 9110
GATEWAY = "http://localhost:8642/v1/chat/completions"
SESSION_ID = "user"
MODEL = "deepseek-v4-flash"

app = FastAPI()

INDEX_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")

@app.get("/")
async def index():
    return HTMLResponse(INDEX_HTML)

@app.post("/api/chat")
async def chat(req: Request):
    data = await req.json()
    message = data.get("message", "").strip()
    stream = data.get("stream", True)
    if not message:
        return JSONResponse({"error": "empty message"}, status_code=400)

    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": message}],
        "session_id": SESSION_ID,
        "stream": stream,
    }

    async with httpx.AsyncClient(timeout=120) as client:
        if stream:
            return StreamingResponse(
                stream_chat(client, payload),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            )
        resp = await client.post(GATEWAY, json=payload)
        data = resp.json()
        reply = ""
        for ch in data.get("choices", []):
            reply += ch.get("delta", {}).get("content", "") or ch.get("message", {}).get("content", "")
        return JSONResponse({"reply": reply})

async def stream_chat(client, payload):
    async with client.stream("POST", GATEWAY, json=payload) as resp:
        buf = ""
        async for chunk in resp.aiter_bytes():
            buf += chunk.decode()
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.strip()
                if not line or line == "data: [DONE]":
                    yield f"data: {json.dumps({'done': True})}\n\n"
                    break
                if line.startswith("data: "):
                    try:
                        d = json.loads(line[6:])
                        content = d.get("choices", [{}])[0].get("delta", {}).get("content", "")
                        if content:
                            yield f"data: {json.dumps({'content': html.escape(content)})}\n\n"
                    except:
                        pass

@app.get("/api/health")
async def health():
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get("http://localhost:8642/health")
            gw = "ok" if r.status_code == 200 else "fail"
    except:
        gw = "unreachable"
    return {"gateway": gw, "session": SESSION_ID}

if __name__ == "__main__":
    print(f"🌐 WebChat at http://{HOST}:{PORT}  (session={SESSION_ID})")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
