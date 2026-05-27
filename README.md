
Agent Message Bus / 消息总线

一个轻量的多 Agent 消息总线。SQLite 做存储，HTTP 协议通信，Agent 自己拉任务、自己上报状态。没有 Redis，没有 Kafka，没有外部依赖。跑起来就三行命令。

可以用来让多个 AI Agent 协同工作：一个 Agent 发任务，其他 Agent 轮询领取，处理完了 ack 回去。状态变化写进事件日志，面板实时能看到谁在线、谁在忙、消息卡在哪个队列。

A lightweight message bus for multi-agent systems. SQLite-backed, HTTP-native. Agents poll for tasks, claim them atomically, and report status back. Zero external dependencies — Python stdlib and a dream.

---

怎么跑起来 / Getting Started

先启动总线：

    python3 anyue_bus_http.py

然后启动调度器（如果不需要路由可以直接跳过，直接往总线 POST 就行）：

    python3 dispatcher.py

启动一个 Worker：

    python3 anyue_bus_worker.py --agent worker_1 --bus http://127.0.0.1:8648 --auto-reply

想看面板的话：

    python3 -m http.server 8080
    浏览器打开 http://localhost:8080/static/dashboard.html

想看像素办公室面板（可选）：

    cd pixel-office
    npm install
    npm run dev
    # 浏览器打开 http://localhost:3456
    # 或者在主面板点 🎮 办公室 按钮内嵌查看

    PS：生产部署用 `npm run build`，启动 `node dist/server.js`

API 端点

总线在 8648 端口：

/health — 健康检查
/status — 所有 Agent 的心跳和队列统计
/send — 投一个任务进去
/poll?agent=X — Agent X 拉自己队列里的任务
/ack_pending — 认领任务（原子操作，多人抢只会有一个成功）
/ack — 完成任务，带上回复
/events?after_id=N — 增量拉事件流
/heartbeat — Agent 发心跳

调度器在 8655 端口：

/health
/routes — 查看当前路由规则
/dispatch — 发消息进来，调度器按正则规则路由到对应 Agent

任务状态

QUEUED（排队）→ ACK_PENDING（有人认领了）→ RUNNING（正在处理）→ COMPLETED / FAILED / TIMEOUT / CANCELLED

认领是原子的。两个 Agent 同时抢同一个任务，只有一个能拿到，另一个会收到 409。

Task lifecycle: QUEUED → ACK_PENDING → RUNNING → COMPLETED (or FAILED, TIMEOUT, CANCELLED). Claiming is atomic — first agent wins, others get a 409 conflict.

文件说明 / Files

anyue_bus_core.py — 核心逻辑，SQLite、队列、状态机
anyue_bus_http.py — HTTP 服务，把核心逻辑暴露成 REST API
anyue_bus_adapter.py — Worker 基类，封装了 poll / claim / heartbeat
anyue_bus_worker.py — 带 LLM 集成的 Worker，支持 OpenAI 兼容 API，自动调用模型生成回复
anyue_bus_dashboard.py — 面板需要的 API 端点
anyue_bus_shadow.py — 影子模式，测试用
dispatcher.py — 消息路由器，正则匹配规则分发
bus_web_ui.py — FastAPI 写的群聊界面
message_deduplicator.py — 幂等去重
static/dashboard.html — 运维面板，实时看 Agent 状态和事件流
webchat/server.py — Web 聊天服务
pixel-office/ — 像素办公室可视化面板（可选组件）

## 像素办公室 / Pixel Office

办公室面板素材原型出自 [rolandal/pixel-agents-standalone](https://github.com/rolandal/pixel-agents-standalone)（MIT License），在此之上做了汉化、总线集成和行动轨迹可视化改造。

- 四个 Agent 以像素角色在办公室走动
- 人物头上实时显示当前活动状态（思考中 / 执行中 / 空闲）
- 支持缩放、拖拽、布局编辑
- 通过 BusPoller 轮询总线 `/v1/status` 驱动状态变化

测试 / Testing

    pip install pytest
    python -m pytest tests/ -v

适配器和 Worker 的测试需要总线在跑。核心逻辑、Schema、send、poll 的测试是纯单元测试，不需要启动服务：

    python -m pytest tests/ -v -k "not test_adapters and not test_worker"

License: MIT
