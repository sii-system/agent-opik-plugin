/**
 * openclaw-opik-tracer: Thin TS plugin shell.
 *
 * Registers openclaw hooks as triggers only — no data is read from hook payloads.
 * Each hook extracts session context metadata and spawns a Python subprocess
 * that incrementally parses the session JSONL file and emits Opik spans.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";
import { initBridge, firePythonTracer, defaultScriptPath, type BridgeEvent } from "./src/bridge.js";

type SubagentRequesterOrigin = NonNullable<NonNullable<BridgeEvent["subagentDelivery"]>["requesterOrigin"]>;

interface PluginConfig {
  opikUrl?: string;
  opikApiKey?: string;
  opikWorkspace?: string;
  opikProjectName?: string;
  tags?: string[];
  pythonPath?: string;
  includeHistory?: boolean;
  dryRun?: boolean;
}

const sessionKeyBySessionId = new Map<string, string>();
const sessionKeyBySessionFile = new Map<string, string>();
const sessionKeyByAgentSession = new Map<string, string>();

function parseConfig(raw: Record<string, unknown>): PluginConfig {
  return {
    opikUrl: typeof raw.opikUrl === "string" ? raw.opikUrl : undefined,
    opikApiKey: typeof raw.opikApiKey === "string" ? raw.opikApiKey : undefined,
    opikWorkspace: typeof raw.opikWorkspace === "string" ? raw.opikWorkspace : undefined,
    opikProjectName: typeof raw.opikProjectName === "string" ? raw.opikProjectName : undefined,
    tags: Array.isArray(raw.tags) ? raw.tags.filter((t): t is string => typeof t === "string") : undefined,
    pythonPath: typeof raw.pythonPath === "string" ? raw.pythonPath : undefined,
    includeHistory: typeof raw.includeHistory === "boolean" ? raw.includeHistory : undefined,
    dryRun: typeof raw.dryRun === "boolean" ? raw.dryRun : undefined,
  };
}

function resolveSessionFile(agentCtx: Record<string, unknown>): string | undefined {
  // Try direct sessionFile from context (some hooks expose it)
  if (typeof agentCtx.sessionFile === "string" && agentCtx.sessionFile.length > 0) {
    return agentCtx.sessionFile;
  }
  // Fallback: construct from agentId + sessionId
  const agentId = agentCtx.agentId;
  const sessionId = agentCtx.sessionId;
  if (typeof agentId === "string" && typeof sessionId === "string") {
    const home = process.env.HOME ?? process.env.USERPROFILE ?? "/tmp";
    return `${home}/.openclaw/agents/${agentId}/sessions/${sessionId}.jsonl`;
  }
  return undefined;
}

function resolveSessionKey(agentCtx: Record<string, unknown>, sessionFile?: string): string | undefined {
  if (typeof agentCtx.sessionKey === "string" && agentCtx.sessionKey.length > 0) {
    return agentCtx.sessionKey;
  }

  const sessionId = typeof agentCtx.sessionId === "string" ? agentCtx.sessionId : undefined;
  if (sessionId) {
    const bySessionId = sessionKeyBySessionId.get(sessionId);
    if (bySessionId) return bySessionId;
  }

  if (sessionFile) {
    const byFile = sessionKeyBySessionFile.get(sessionFile);
    if (byFile) return byFile;
  }

  const agentId = typeof agentCtx.agentId === "string" ? agentCtx.agentId : undefined;
  if (agentId && sessionId) {
    const byAgentSession = sessionKeyByAgentSession.get(`${agentId}:${sessionId}`);
    if (byAgentSession) return byAgentSession;
  }

  return sessionId;
}

function rememberSessionIdentity(event: BridgeEvent): void {
  if (event.sessionId) {
    sessionKeyBySessionId.set(event.sessionId, event.sessionKey);
  }
  if (event.sessionFile) {
    sessionKeyBySessionFile.set(event.sessionFile, event.sessionKey);
  }
  if (event.agentId && event.sessionId) {
    sessionKeyByAgentSession.set(`${event.agentId}:${event.sessionId}`, event.sessionKey);
  }
}

function buildBaseEvent(
  eventName: string,
  agentCtx: Record<string, unknown>,
  config: PluginConfig,
): BridgeEvent | null {
  const sessionFile = resolveSessionFile(agentCtx);
  if (!sessionFile) return null;

  const sessionKey = resolveSessionKey(agentCtx, sessionFile);
  if (!sessionKey) return null;

  const event: BridgeEvent = {
    event: eventName,
    sessionKey,
    sessionId: typeof agentCtx.sessionId === "string" ? agentCtx.sessionId : undefined,
    agentId: typeof agentCtx.agentId === "string" ? agentCtx.agentId : undefined,
    runId: typeof agentCtx.runId === "string" ? agentCtx.runId : undefined,
    sessionFile,
    channelId: typeof agentCtx.channelId === "string" ? agentCtx.channelId : undefined,
    trigger: typeof agentCtx.trigger === "string" ? agentCtx.trigger : undefined,
    timestamp: Date.now(),
    config: {
      opikUrl: config.opikUrl,
      opikWorkspace: config.opikWorkspace,
      opikProjectName: config.opikProjectName,
      tags: config.tags,
      includeHistory: config.includeHistory,
      dryRun: config.dryRun,
    },
  };
  rememberSessionIdentity(event);
  return event;
}

function buildLightEvent(
  eventName: string,
  agentCtx: Record<string, unknown>,
  config: PluginConfig,
  sessionKey: string,
): BridgeEvent {
  const sessionFile = resolveSessionFile(agentCtx);
  const event: BridgeEvent = {
    event: eventName,
    sessionKey,
    sessionId: typeof agentCtx.sessionId === "string" ? agentCtx.sessionId : undefined,
    agentId: typeof agentCtx.agentId === "string" ? agentCtx.agentId : undefined,
    runId: typeof agentCtx.runId === "string" ? agentCtx.runId : undefined,
    sessionFile,
    channelId: typeof agentCtx.channelId === "string" ? agentCtx.channelId : undefined,
    trigger: typeof agentCtx.trigger === "string" ? agentCtx.trigger : undefined,
    timestamp: Date.now(),
    config: {
      opikUrl: config.opikUrl,
      opikWorkspace: config.opikWorkspace,
      opikProjectName: config.opikProjectName,
      tags: config.tags,
      includeHistory: config.includeHistory,
      dryRun: config.dryRun,
    },
  };
  rememberSessionIdentity(event);
  return event;
}

function buildSessionStartBridgeEvent(
  agentCtx: Record<string, unknown>,
  event: Record<string, unknown>,
  config: PluginConfig,
): BridgeEvent | null {
  const sessionKey = resolveSessionKey(agentCtx);
  if (!sessionKey) return null;
  const bridgeEvent = buildLightEvent("session_start", agentCtx, config, sessionKey);
  bridgeEvent.resumedFrom = typeof event.resumedFrom === "string" ? event.resumedFrom : undefined;
  return bridgeEvent;
}

function buildSubagentSpawningBridgeEvent(
  agentCtx: Record<string, unknown>,
  event: Record<string, unknown>,
  config: PluginConfig,
): BridgeEvent | null {
  const parentSessionKey =
    typeof agentCtx.requesterSessionKey === "string" && agentCtx.requesterSessionKey.length > 0
      ? agentCtx.requesterSessionKey
      : undefined;
  if (!parentSessionKey) return null;
  const bridgeEvent = buildLightEvent("subagent_spawning", agentCtx, config, parentSessionKey);
  bridgeEvent.childSessionKey = typeof event.childSessionKey === "string" ? event.childSessionKey : undefined;
  bridgeEvent.childAgentId = typeof event.agentId === "string" ? event.agentId : undefined;
  bridgeEvent.subagentLabel = typeof event.label === "string" ? event.label : undefined;
  bridgeEvent.subagentMode = event.mode === "run" || event.mode === "session" ? event.mode : undefined;
  return bridgeEvent;
}

function buildSubagentDeliveryBridgeEvent(
  agentCtx: Record<string, unknown>,
  event: Record<string, unknown>,
  config: PluginConfig,
): BridgeEvent | null {
  const parentSessionKey =
    typeof agentCtx.requesterSessionKey === "string" && agentCtx.requesterSessionKey.length > 0
      ? agentCtx.requesterSessionKey
      : typeof event.requesterSessionKey === "string" && event.requesterSessionKey.length > 0
        ? event.requesterSessionKey
        : undefined;
  if (!parentSessionKey) return null;
  const bridgeEvent = buildLightEvent("subagent_delivery_target", agentCtx, config, parentSessionKey);
  bridgeEvent.childSessionKey = typeof event.childSessionKey === "string" ? event.childSessionKey : undefined;
  bridgeEvent.subagentDelivery = {
    requesterSessionKey:
      typeof event.requesterSessionKey === "string" ? event.requesterSessionKey : undefined,
    spawnMode: event.spawnMode === "run" || event.spawnMode === "session" ? event.spawnMode : undefined,
    expectsCompletionMessage:
      typeof event.expectsCompletionMessage === "boolean" ? event.expectsCompletionMessage : undefined,
    requesterOrigin:
      event.requesterOrigin && typeof event.requesterOrigin === "object"
        ? event.requesterOrigin as SubagentRequesterOrigin
        : undefined,
  };
  return bridgeEvent;
}

const plugin = {
  id: "openclaw-opik-tracer",
  name: "Opik Tracer (JSONL)",
  description: "Trace openclaw sessions to Opik via incremental JSONL parsing",
  configSchema: emptyPluginConfigSchema(),

  register(api: OpenClawPluginApi) {
    const config = parseConfig(api.pluginConfig as Record<string, unknown>);
    const pythonPath = config.pythonPath ?? "python3";
    const scriptPath = defaultScriptPath();

    // Build env vars for Python process
    const env: Record<string, string> = {};
    if (config.opikUrl) env.OPIK_URL_OVERRIDE = config.opikUrl;
    if (config.opikApiKey) env.OPIK_API_KEY = config.opikApiKey;
    if (config.opikWorkspace) env.OPIK_WORKSPACE = config.opikWorkspace;
    if (config.opikProjectName) env.OPIK_PROJECT_NAME = config.opikProjectName;
    if (config.dryRun) env.OC_OPIK_DRY_RUN = "true";

    let warn = (msg: string) => console.warn(msg);

    api.registerService({
      id: "opik-tracer",

      async start(ctx) {
        warn = ctx.logger.warn.bind(ctx.logger);
        initBridge({ pythonPath, scriptPath, env, warn });
        ctx.logger.info(`opik-tracer: initialized (script=${scriptPath}, python=${pythonPath})`);
      },

      async stop() {
        // Python processes are fire-and-forget, nothing to clean up
      },
    });

    // ---------------------------------------------------------------
    // Hook: llm_input — record session is active, no JSONL parse yet
    // ---------------------------------------------------------------
    api.on("llm_input", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("llm_input", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.model = typeof (event as any).model === "string" ? (event as any).model : undefined;
      bridgeEvent.provider = typeof (event as any).provider === "string" ? (event as any).provider : undefined;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: llm_output — trigger incremental JSONL parse & emit
    // ---------------------------------------------------------------
    api.on("llm_output", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("llm_output", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.model = typeof (event as any).model === "string" ? (event as any).model : undefined;
      bridgeEvent.provider = typeof (event as any).provider === "string" ? (event as any).provider : undefined;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: before_agent_reply — fallback-safe flush before reply persists
    // Some embedded/failover paths can produce a final assistant message
    // without delivering a usable agent_end signal to this plugin.
    // ---------------------------------------------------------------
    api.on("before_agent_reply", (_event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("llm_output", ctx, config);
      if (!bridgeEvent) return;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: after_tool_call — trigger incremental JSONL parse & emit
    // ---------------------------------------------------------------
    api.on("after_tool_call", (event, toolCtx) => {
      const ctx = toolCtx as Record<string, unknown>;
      // after_tool_call may lack sessionKey — try agentId fallback for file path
      const bridgeEvent = buildBaseEvent("after_tool_call", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.toolName = typeof event.toolName === "string" ? event.toolName : undefined;
      bridgeEvent.toolCallId = typeof event.toolCallId === "string" ? event.toolCallId : undefined;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: agent_end — final flush, finalize trace
    // ---------------------------------------------------------------
    api.on("agent_end", (_event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("agent_end", ctx, config);
      if (!bridgeEvent) return;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: session_end — final fallback flush when session lifecycle ends
    // ---------------------------------------------------------------
    api.on("session_end", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("session_end", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.sessionEndReason = typeof event.reason === "string" ? event.reason : undefined;
      bridgeEvent.sessionEndMessageCount = typeof event.messageCount === "number" ? event.messageCount : undefined;
      bridgeEvent.sessionEndDurationMs = typeof event.durationMs === "number" ? event.durationMs : undefined;
      bridgeEvent.nextSessionId = typeof event.nextSessionId === "string" ? event.nextSessionId : undefined;
      bridgeEvent.nextSessionKey = typeof event.nextSessionKey === "string" ? event.nextSessionKey : undefined;
      bridgeEvent.transcriptArchived =
        typeof event.transcriptArchived === "boolean" ? event.transcriptArchived : undefined;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: session_start — create/open trace before first llm call
    // ---------------------------------------------------------------
    api.on("session_start", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildSessionStartBridgeEvent(ctx, event as Record<string, unknown>, config);
      if (!bridgeEvent) return;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: before_agent_start — legacy control-plane hook, observe only.
    // Must always return undefined so the tracer never mutates runtime behavior.
    // ---------------------------------------------------------------
    api.on("before_agent_start", (event, agentCtx) => {
      try {
        const ctx = agentCtx as Record<string, unknown>;
        const bridgeEvent = buildBaseEvent("before_agent_start", ctx, config);
        if (bridgeEvent) {
          bridgeEvent.model = typeof (event as { modelOverride?: unknown }).modelOverride === "string"
            ? (event as { modelOverride?: string }).modelOverride
            : undefined;
          bridgeEvent.provider = typeof (event as { providerOverride?: unknown }).providerOverride === "string"
            ? (event as { providerOverride?: string }).providerOverride
            : undefined;
          firePythonTracer(bridgeEvent);
        }
      } catch (error) {
        warn(`opik-tracer: before_agent_start hook failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      return undefined;
    });

    // ---------------------------------------------------------------
    // Hook: before_compaction — open compaction span
    // ---------------------------------------------------------------
    api.on("before_compaction", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("before_compaction", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.compaction = {
        messageCount: typeof event.messageCount === "number" ? event.messageCount : undefined,
        compactingCount: typeof event.compactingCount === "number" ? event.compactingCount : undefined,
        tokenCount: typeof event.tokenCount === "number" ? event.tokenCount : undefined,
      };
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: after_compaction — close compaction span
    // ---------------------------------------------------------------
    api.on("after_compaction", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("after_compaction", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.compaction = {
        messageCount: typeof event.messageCount === "number" ? event.messageCount : undefined,
        tokenCount: typeof event.tokenCount === "number" ? event.tokenCount : undefined,
        compactedCount: typeof event.compactedCount === "number" ? event.compactedCount : undefined,
      };
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: before_reset — finalize trace ahead of reset
    // ---------------------------------------------------------------
    api.on("before_reset", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("before_reset", ctx, config);
      if (!bridgeEvent) return;
      bridgeEvent.resetReason = typeof event.reason === "string" ? event.reason : undefined;
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: before_tool_call — control-plane hook, observe only.
    // Must always return undefined so the tracer never blocks or mutates tools.
    // ---------------------------------------------------------------
    api.on("before_tool_call", (event, toolCtx) => {
      try {
        const ctx = toolCtx as Record<string, unknown>;
        const bridgeEvent = buildBaseEvent("before_tool_call", ctx, config);
        if (bridgeEvent) {
          bridgeEvent.toolName = typeof event.toolName === "string" ? event.toolName : undefined;
          bridgeEvent.toolCallId = typeof event.toolCallId === "string" ? event.toolCallId : undefined;
          firePythonTracer(bridgeEvent);
        }
      } catch (error) {
        warn(`opik-tracer: before_tool_call hook failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      return undefined;
    });

    // ---------------------------------------------------------------
    // Hook: subagent_spawned — register subagent for tracking
    // ---------------------------------------------------------------
    api.on("subagent_spawned", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("subagent_spawned", ctx, config);
      if (!bridgeEvent) return;
      const eventObj = event as Record<string, unknown>;
      if (typeof eventObj.childSessionKey === "string") {
        (bridgeEvent as any).childSessionKey = eventObj.childSessionKey;
      }
      if (typeof eventObj.agentId === "string") {
        (bridgeEvent as any).childAgentId = eventObj.agentId;
      }
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: subagent_spawning — control-plane hook, observe only.
    // Must always return undefined so the tracer never affects spawn outcomes.
    // ---------------------------------------------------------------
    api.on("subagent_spawning", (event, agentCtx) => {
      try {
        const ctx = agentCtx as Record<string, unknown>;
        const bridgeEvent = buildSubagentSpawningBridgeEvent(ctx, event as Record<string, unknown>, config);
        if (!bridgeEvent) {
          warn("opik-tracer: subagent_spawning skipped because requesterSessionKey is missing");
          return undefined;
        }
        firePythonTracer(bridgeEvent);
      } catch (error) {
        warn(`opik-tracer: subagent_spawning hook failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      return undefined;
    });

    // ---------------------------------------------------------------
    // Hook: subagent_delivery_target — record parent-side delivery routing
    // ---------------------------------------------------------------
    api.on("subagent_delivery_target", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildSubagentDeliveryBridgeEvent(ctx, event as Record<string, unknown>, config);
      if (!bridgeEvent) {
        warn("opik-tracer: subagent_delivery_target skipped because requesterSessionKey is missing");
        return;
      }
      firePythonTracer(bridgeEvent);
    });

    // ---------------------------------------------------------------
    // Hook: subagent_ended — finalize subagent spans
    // ---------------------------------------------------------------
    api.on("subagent_ended", (event, agentCtx) => {
      const ctx = agentCtx as Record<string, unknown>;
      const bridgeEvent = buildBaseEvent("subagent_ended", ctx, config);
      if (!bridgeEvent) return;
      const eventObj = event as Record<string, unknown>;
      if (typeof eventObj.targetSessionKey === "string") {
        (bridgeEvent as any).childSessionKey = eventObj.targetSessionKey;
      }
      if (eventObj.targetKind === "subagent" || eventObj.targetKind === "acp") {
        bridgeEvent.subagentTargetKind = eventObj.targetKind;
      }
      if (typeof eventObj.reason === "string") {
        bridgeEvent.subagentEndReason = eventObj.reason;
      }
      if (
        eventObj.outcome === "ok"
        || eventObj.outcome === "error"
        || eventObj.outcome === "timeout"
        || eventObj.outcome === "killed"
        || eventObj.outcome === "reset"
        || eventObj.outcome === "deleted"
      ) {
        bridgeEvent.subagentOutcome = eventObj.outcome;
      }
      if (typeof eventObj.endedAt === "string") {
        bridgeEvent.subagentEndedAt = eventObj.endedAt;
      }
      if (typeof eventObj.error === "string") {
        bridgeEvent.subagentError = eventObj.error;
      }
      if (typeof eventObj.sendFarewell === "boolean") {
        bridgeEvent.subagentSendFarewell = eventObj.sendFarewell;
      }
      firePythonTracer(bridgeEvent);
    });
  },
};

export const __testInternals = {
  parseConfig,
  resolveSessionKey,
  resolveSessionFile,
  buildBaseEvent,
  buildLightEvent,
  buildSessionStartBridgeEvent,
  buildSubagentSpawningBridgeEvent,
  buildSubagentDeliveryBridgeEvent,
  rememberSessionIdentity,
};

export default plugin;
