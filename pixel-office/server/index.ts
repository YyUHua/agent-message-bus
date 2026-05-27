import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { join, dirname } from "path";
import { homedir } from "os";
import { fileURLToPath } from "url";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import { BusPoller } from "./bus-poller.js";
import {
  loadCharacterSprites,
  loadWallTiles,
  loadFloorTiles,
  loadFurnitureAssets,
  loadDefaultLayout,
} from "./assetLoader.js";
import type { TrackedAgent, ServerMessage, AgentActivity } from "./types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.PORT || "3456", 10);

// State
const agents = new Map<string, TrackedAgent>();
const clients = new Set<WebSocket>();
const busToPixelId = new Map<string, number>();

// Agent config from bus-poller
interface AgentConfig {
  id: number;
  busId: string;
  name: string;
  palette: number;
  hueShift: number;
}

let agentConfigs: AgentConfig[] = [];

// Load assets
const devAssetsRoot = join(__dirname, "..", "webview-ui", "public", "assets");
const prodAssetsRoot = join(__dirname, "public", "assets");
const assetsRoot = existsSync(devAssetsRoot) ? devAssetsRoot : prodAssetsRoot;

console.log(`[Server] Loading assets from: ${assetsRoot}`);

const characterSprites = loadCharacterSprites(assetsRoot);
const wallTiles = loadWallTiles(assetsRoot);
const floorTiles = loadFloorTiles(assetsRoot);
const furnitureAssets = loadFurnitureAssets(assetsRoot);

// Persistence
const persistDir = join(homedir(), ".pixel-agents");
const persistedLayoutPath = join(persistDir, "layout.json");
const persistedSeatsPath = join(persistDir, "agent-seats.json");

function loadLayout(): Record<string, unknown> | null {
  if (existsSync(persistedLayoutPath)) {
    try {
      const content = readFileSync(persistedLayoutPath, "utf-8");
      return JSON.parse(content);
    } catch {
      /* ignore */
    }
  }
  return loadDefaultLayout(assetsRoot);
}

function loadPersistedSeats(): Record<number, { palette: number; hueShift: number; seatId: string | null }> | null {
  if (existsSync(persistedSeatsPath)) {
    try {
      return JSON.parse(readFileSync(persistedSeatsPath, "utf-8"));
    } catch {
      return null;
    }
  }
  return null;
}

let currentLayout = loadLayout();
const persistedSeats = loadPersistedSeats();

// Express
const app = express();
app.use(express.static(join(__dirname, "public")));

const server = createServer(app);
const wss = new WebSocketServer({ server });

// Heartbeat
const HEARTBEAT_INTERVAL_MS = 30_000;
setInterval(() => {
  for (const ws of clients) {
    if ((ws as unknown as Record<string, boolean>).__isAlive === false) {
      clients.delete(ws);
      ws.terminate();
      continue;
    }
    (ws as unknown as Record<string, boolean>).__isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL_MS);

function broadcast(msg: ServerMessage): void {
  const data = JSON.stringify(msg);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

// Activity timers: auto-idle after 5s of no tool activity
const activityTimers = new Map<number, ReturnType<typeof setTimeout>>();

function setAgentActivity(id: number, activity: AgentActivity): void {
  // Clear previous timer
  const prev = activityTimers.get(id);
  if (prev) clearTimeout(prev);
  
  broadcast({ type: "agentStatus", id, status: activity });
  
  // Auto-idle after 8s
  if (activity !== "idle") {
    activityTimers.set(id, setTimeout(() => {
      broadcast({ type: "agentStatus", id, status: "idle" });
      activityTimers.delete(id);
    }, 8000));
  }
}

function sendInitialData(ws: WebSocket): void {
  ws.send(JSON.stringify({ type: "settingsLoaded", soundEnabled: false }));

  if (characterSprites) {
    ws.send(JSON.stringify({ type: "characterSpritesLoaded", characters: characterSprites.characters }));
  }
  if (wallTiles) {
    ws.send(JSON.stringify({ type: "wallTilesLoaded", sprites: wallTiles.sprites }));
  }
  if (floorTiles) {
    ws.send(JSON.stringify({ type: "floorTilesLoaded", sprites: floorTiles.sprites }));
  }
  if (furnitureAssets) {
    ws.send(JSON.stringify({
      type: "furnitureAssetsLoaded",
      catalog: furnitureAssets.catalog,
      sprites: furnitureAssets.sprites,
    }));
  }

  // Send existing agents with their config (palette, hueShift, name)
  const agentList = Array.from(agents.values());
  const folderNames: Record<number, string> = {};
  const agentMeta: Record<number, { palette?: number; hueShift?: number; seatId?: string }> = {};
  
  for (const a of agentList) {
    const cfg = agentConfigs.find(c => c.id === a.id);
    folderNames[a.id] = cfg?.name || a.projectName;
    if (persistedSeats?.[a.id]) {
      agentMeta[a.id] = {
        palette: persistedSeats[a.id].palette,
        hueShift: persistedSeats[a.id].hueShift,
        seatId: persistedSeats[a.id].seatId ?? undefined,
      };
    } else if (cfg) {
      // Use our fixed palette/hue from agent config
      agentMeta[a.id] = { palette: cfg.palette, hueShift: cfg.hueShift };
    }
  }
  
  ws.send(JSON.stringify({
    type: "existingAgents",
    agents: agentList.map(a => a.id),
    folderNames,
    agentMeta,
  }));

  if (currentLayout) {
    ws.send(JSON.stringify({ type: "layoutLoaded", layout: currentLayout, version: 1 }));
  } else {
    ws.send(JSON.stringify({ type: "layoutLoaded", layout: null, version: 0 }));
  }
}

wss.on("connection", (ws) => {
  (ws as unknown as Record<string, boolean>).__isAlive = true;
  ws.on("pong", () => { (ws as unknown as Record<string, boolean>).__isAlive = true; });
  clients.add(ws);

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "webviewReady" || msg.type === "ready") {
        sendInitialData(ws);
      } else if (msg.type === "saveLayout") {
        try {
          mkdirSync(persistDir, { recursive: true });
          writeFileSync(persistedLayoutPath, JSON.stringify(msg.layout, null, 2));
          currentLayout = msg.layout;
          broadcast({ type: "layoutLoaded", layout: msg.layout, version: 1 });
        } catch {
          /* ignore */
        }
      } else if (msg.type === "saveAgentSeats") {
        try {
          mkdirSync(persistDir, { recursive: true });
          writeFileSync(persistedSeatsPath, JSON.stringify(msg.seats, null, 2));
        } catch {
          /* ignore */
        }
      }
    } catch {
      /* ignore invalid messages */
    }
  });

  ws.on("close", () => clients.delete(ws));
});

// Bus Poller — replaces JsonlWatcher
const poller = new BusPoller();

poller.on("agentCreated", (data: { id: number; busId: string; name: string; palette: number; hueShift: number }) => {
  if (agents.has(data.busId)) return;

  const agent: TrackedAgent = {
    id: data.id,
    sessionId: data.busId,
    projectDir: "",
    projectName: data.name,
    jsonlFile: "",
    fileOffset: 0,
    lineBuffer: "",
    activity: "idle",
    activeTools: new Map(),
    activeToolNames: new Map(),
    activeSubagentToolIds: new Map(),
    activeSubagentToolNames: new Map(),
    isWaiting: false,
    permissionSent: false,
    hadToolsInTurn: false,
    lastActivityTime: Date.now(),
  };

  agents.set(data.busId, agent);
  busToPixelId.set(data.busId, data.id);
  
  if (!agentConfigs.find(c => c.id === data.id)) {
    agentConfigs.push(data);
  }

  broadcast({ type: "agentCreated", id: agent.id, folderName: agent.projectName });
  console.log(`[Server] Agent ${agent.id} joined: ${agent.projectName} (${data.busId})`);
});

poller.on("agentActivity", (data: { id: number; busId: string; activity: AgentActivity }) => {
  setAgentActivity(data.id, data.activity);
});

poller.on("agentToolStart", (data: { id: number; toolId: string; toolName: string; status: string }) => {
  broadcast({ type: "agentToolStart", id: data.id, toolId: data.toolId, status: data.status });
  setAgentActivity(data.id, "typing");
});

poller.on("agentToolDone", (data: { id: number; toolId: string }) => {
  broadcast({ type: "agentToolDone", id: data.id, toolId: data.toolId });
});

poller.on("agentClosed", (data: { id: number; busId: string }) => {
  agents.delete(data.busId);
  broadcast({ type: "agentClosed", id: data.id });
  console.log(`[Server] Agent ${data.id} left`);
});

// Start
poller.start();
server.listen(PORT, () => {
  console.log(`Agent Office server at http://localhost:${PORT}`);
  console.log(`Connected to message bus at ${process.env.BUS_URL || "http://127.0.0.1:8648"}`);
});
