/**
 * Bridge: spawn Python tracer subprocess with event data on stdin.
 * Fire-and-forget — never blocks the openclaw agent loop.
 */

import { spawn } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));

export interface BridgeEvent {
  event: string;
  sessionKey: string;
  sessionId?: string;
  agentId?: string;
  runId?: string;
  sessionFile?: string;
  model?: string;
  provider?: string;
  channelId?: string;
  trigger?: string;
  toolName?: string;
  toolCallId?: string;
  childSessionKey?: string;
  childAgentId?: string;
  resumedFrom?: string;
  resetReason?: string;
  sessionEndReason?: string;
  sessionEndMessageCount?: number;
  sessionEndDurationMs?: number;
  nextSessionId?: string;
  nextSessionKey?: string;
  transcriptArchived?: boolean;
  compaction?: {
    messageCount?: number;
    compactingCount?: number;
    tokenCount?: number;
    compactedCount?: number;
  };
  subagentLabel?: string;
  subagentMode?: "run" | "session";
  subagentTargetKind?: "subagent" | "acp";
  subagentEndReason?: string;
  subagentOutcome?: "ok" | "error" | "timeout" | "killed" | "reset" | "deleted";
  subagentEndedAt?: string;
  subagentError?: string;
  subagentSendFarewell?: boolean;
  subagentDelivery?: {
    requesterSessionKey?: string;
    spawnMode?: "run" | "session";
    expectsCompletionMessage?: boolean;
    requesterOrigin?: {
      channel?: string;
      accountId?: string;
      to?: string;
      threadId?: string | number;
    };
  };
  timestamp: number;
  // Plugin config forwarded to Python
  config?: Record<string, unknown>;
}

export interface BridgeOptions {
  pythonPath: string;
  scriptPath: string;
  env?: Record<string, string>;
  warn: (msg: string) => void;
}

let defaultOptions: BridgeOptions | null = null;

export function initBridge(opts: BridgeOptions): void {
  defaultOptions = opts;
}

export function firePythonTracer(event: BridgeEvent, opts?: BridgeOptions): void {
  const o = opts ?? defaultOptions;
  if (!o) return;

  try {
    const child = spawn(o.pythonPath, [o.scriptPath], {
      stdio: ["pipe", "ignore", "pipe"],
      detached: true,
      env: {
        ...process.env,
        ...o.env,
      },
    });

    let stderrBuf = "";
    child.stderr?.on("data", (chunk: Buffer) => {
      stderrBuf += chunk.toString();
      // Cap buffer to avoid memory issues on runaway stderr
      if (stderrBuf.length > 4096) {
        stderrBuf = stderrBuf.slice(-2048);
      }
    });

    child.on("error", (err) => {
      o.warn(`opik-tracer: failed to spawn python: ${err.message}`);
    });

    child.on("exit", (code) => {
      if (code !== 0 && code !== null) {
        const snippet = stderrBuf.trim().slice(0, 500);
        o.warn(`opik-tracer: python exited ${code}${snippet ? `: ${snippet}` : ""}`);
      }
    });

    child.stdin.write(JSON.stringify(event));
    child.stdin.end();
    child.unref();
  } catch (err) {
    o.warn(`opik-tracer: bridge error: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/**
 * Resolve the default path to the Python tracer script.
 * Lives at src/sii_opik_plugin/openclaw/openclaw_opik_tracer.py
 * at the repo root, regardless of whether this file runs from source
 * (harness/openclaw/src/bridge.ts) or built (harness/openclaw/dist/src/bridge.js).
 */
export function defaultScriptPath(): string {
  const candidates = [
    // Built mode: harness/openclaw/dist/src/bridge.js → up 4 → repo root
    resolve(__dirname, "..", "..", "..", "..", "src", "sii_opik_plugin", "openclaw", "openclaw_opik_tracer.py"),
    // Source mode: harness/openclaw/src/bridge.ts → up 3 → repo root
    resolve(__dirname, "..", "..", "..", "src", "sii_opik_plugin", "openclaw", "openclaw_opik_tracer.py"),
  ];
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return candidates[0];
}
