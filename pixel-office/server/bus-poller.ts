/**
 * Bus Poller — replaces JsonlWatcher for multi-agent bus integration.
 * Polls the message bus API and maps agent states to pixel character events.
 */
import { EventEmitter } from "events";

const BUS_URL = process.env.BUS_URL || "http://127.0.0.1:8648";
const POLL_INTERVAL_MS = 2000;
const OFFLINE_THRESHOLD_MS = 120_000; // 2 min no heartbeat = offline

interface BusAgent {
  agent_id: string;
  status: string;
  last_heartbeat: number;
}

interface BusEvent {
  event_id: number;
  agent: string;
  type: string;
  task_id?: string;
  summary?: string;
  tool?: string;
  ts: number;
}

// Fixed agent pool
const KNOWN_AGENTS = [
  { busId: "agent_alpha",  name: "Agent Alpha", palette: 2, hueShift: 0 },
  { busId: "agent_beta",   name: "Agent Beta",  palette: 0, hueShift: 0 },
  { busId: "agent_gamma",  name: "Agent Gamma", palette: 4, hueShift: 0 },
  { busId: "agent_delta",  name: "Agent Delta", palette: 3, hueShift: 60 },
];

export class BusPoller extends EventEmitter {
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private agentIds = new Map<string, number>(); // busId -> pixel agent id
  private knownAgents = new Set<string>();
  private lastSeen = new Map<string, number>(); // busId -> last seen timestamp
  private lastEventId = 0;
  private prevStatus = new Map<string, string>(); // busId -> previous status
  private activeTools = new Map<string, string>(); // busId -> current tool name

  start(): void {
    // Assign fixed IDs
    let nextId = 1;
    for (const a of KNOWN_AGENTS) {
      this.agentIds.set(a.busId, nextId);
      nextId++;
    }

    this.pollOnce();
    this.pollTimer = setInterval(() => this.pollOnce(), POLL_INTERVAL_MS);
    console.log(`[BusPoller] Polling ${BUS_URL} every ${POLL_INTERVAL_MS}ms`);
  }

  stop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private async pollOnce(): Promise<void> {
    try {
      await Promise.all([this.pollStatus(), this.pollEvents()]);
    } catch (err) {
      // Silently retry next cycle
    }
  }

  private async pollStatus(): Promise<void> {
    try {
      const res = await fetch(`${BUS_URL}/v1/status`);
      if (!res.ok) return;
      const data = await res.json() as { agents?: BusAgent[]; timestamp?: number };
      const agents = data.agents || [];
      const now = Date.now();

      for (const a of agents) {
        if (!this.agentIds.has(a.agent_id)) continue;
        const id = this.agentIds.get(a.agent_id)!;
        const agent = KNOWN_AGENTS.find(k => k.busId === a.agent_id)!;
        
        this.lastSeen.set(a.agent_id, now);

        if (!this.knownAgents.has(a.agent_id)) {
          // New agent online
          this.knownAgents.add(a.agent_id);
          this.emit("agentCreated", {
            id,
            busId: a.agent_id,
            name: agent.name,
            palette: agent.palette,
            hueShift: agent.hueShift,
          });
          console.log(`[BusPoller] Agent ${id} (${agent.name}) joined`);
        }

        // Map bus status to pixel activity
        const activity = this.mapStatus(a.status);
        this.emit("agentActivity", {
          id,
          busId: a.agent_id,
          status: a.status,
          activity,
        });

        // On every poll, report active status as a tool — so late-connecting clients see it.
        // Frontend deduplicates by toolId, so emitting every cycle is safe.
        if (a.status === "thinking" || a.status === "running" || a.status === "active") {
          const toolName = this.statusToLabel(a.status);
          const agent = KNOWN_AGENTS.find(k => k.busId === a.agent_id)!;
          this.emit("agentToolStart", {
            id,
            toolId: `${a.agent_id}-${toolName}`,
            toolName,
            status: `${agent.name} ${toolName}`,
          });
          this.activeTools.set(a.agent_id, toolName);
        } else {
          // Agent is idle — clear any active tool
          const oldTool = this.activeTools.get(a.agent_id);
          if (oldTool) {
            this.emit("agentToolDone", { id, toolId: `${a.agent_id}-${oldTool}` });
            this.activeTools.delete(a.agent_id);
          }
        }

        // Track previous status (unused now, kept for potential future use)
        this.prevStatus.set(a.agent_id, a.status);
      }

      // Check for offline agents
      for (const [busId, lastTime] of Array.from(this.lastSeen.entries())) {
        if (now - lastTime > OFFLINE_THRESHOLD_MS && this.knownAgents.has(busId)) {
          const id = this.agentIds.get(busId);
          if (id) {
            this.knownAgents.delete(busId);
            this.emit("agentClosed", { id, busId });
            console.log(`[BusPoller] Agent ${id} went offline`);
          }
        }
      }
    } catch {
      // Bus may be down
    }
  }

  private async pollEvents(): Promise<void> {
    try {
      const res = await fetch(`${BUS_URL}/v1/events?after_id=${this.lastEventId}`);
      if (!res.ok) return;
      const data = await res.json() as { events?: BusEvent[] };
      const events = data.events || [];

      for (const evt of events) {
        if (evt.event_id > this.lastEventId) {
          this.lastEventId = evt.event_id;
        }

        if (!this.agentIds.has(evt.agent)) continue;
        const id = this.agentIds.get(evt.agent)!;

        // Map event type to tool activity
        if (evt.type === "running" || evt.type === "thinking") {
          const toolName = evt.tool || "Working";
          const status = evt.summary || `${toolName}...`;
          this.emit("agentToolStart", { id, toolId: evt.task_id || `evt-${evt.event_id}`, toolName, status });
        } else if (evt.type === "completed" || evt.type === "acked") {
          if (evt.task_id) {
            this.emit("agentToolDone", { id, toolId: evt.task_id });
          }
        }
      }
    } catch {
      // Events endpoint may be unavailable
    }
  }

  private mapStatus(status: string): "idle" | "typing" | "reading" | "waiting" {
    switch (status) {
      case "thinking":
      case "running":
      case "active":
        return "typing";
      case "ok":
      case "online":
      case "idle":
        return "idle";
      default:
        return "waiting";
    }
  }

  private statusToLabel(status: string): string {
    switch (status) {
      case "thinking": return "思考中";
      case "running":  return "执行中";
      case "active":   return "工作中";
      default:         return "处理中";
    }
  }
}
