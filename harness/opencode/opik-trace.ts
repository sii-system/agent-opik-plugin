// OpenCode plugin: spawn the realtime trace hook on lifecycle events.
//
// Each event spawns a detached `python3 opencode_realtime_hook.py <event>` with
// a one-shot JSON payload on stdin. stdout/stderr go to /dev/null and the child
// is unref'd, so OpenCode never blocks on or sees output from the hook.
// Failures are swallowed (fail-open) per design.
//
// Resolves the hook script via OPENCODE_OPIK_HOOK_SCRIPT env, otherwise the
// default location next to this plugin file.
//
// Hooks fired:
//   - tool.execute.after              -> "tool_complete"  (5s throttled in Python)
//   - experimental.session.compacting -> "session_compacting"
//   - event(message.updated, role=user) -> "user_prompt"
//   - event(session.idle)              -> "session_idle"
//   - event(session.completed)         -> "session_end"
//
// References:
//   DESIGN_opencode_realtime_trace.md §2 (hook events) and §2.1 (plugin sketch)

import { spawn } from "node:child_process"
import { appendFileSync, mkdirSync } from "node:fs"
import { homedir } from "node:os"
import { dirname, resolve } from "node:path"

const HOOK =
  process.env.OPENCODE_OPIK_HOOK_SCRIPT ??
  resolve(homedir(), ".config/opencode/plugins/opencode_realtime_hook.py")

const PYTHON = process.env.OPENCODE_OPIK_PYTHON ?? "python3"
const PLUGIN_LOG = resolve(homedir(), ".opencode/state/opik_plugin.log")
let fireQueue = Promise.resolve()

function pluginDebugEnabled(): boolean {
  return (
    process.env.OC_OPIK_DEBUG?.toLowerCase() === "true" ||
    process.env.OC_OPIK_PLUGIN_DEBUG?.toLowerCase() === "true"
  )
}

function log(message: string, force = false): void {
  if (!force && !pluginDebugEnabled()) return
  try {
    mkdirSync(dirname(PLUGIN_LOG), { recursive: true })
    appendFileSync(PLUGIN_LOG, `${new Date().toISOString()} ${message}\n`)
  } catch {
    /* fail-open: tracing must never break OpenCode */
  }
}

function envSummary(): string {
  return [
    `debug=${process.env.OC_OPIK_DEBUG ?? ""}`,
    `dry_run=${process.env.OC_OPIK_DRY_RUN ?? ""}`,
    `trace=${process.env.TRACE_TO_OPIK ?? ""}`,
    `db=${process.env.OPENCODE_DB_PATH ?? ""}`,
    `project=${process.env.OPIK_PROJECT_NAME ?? process.env.OC_OPIK_PROJECT ?? ""}`,
  ].join(" ")
}

function fire(event: string, payload: Record<string, unknown>): Promise<void> {
  fireQueue = fireQueue.then(() => fireNow(event, payload), () => fireNow(event, payload))
  return fireQueue
}

function fireNow(event: string, payload: Record<string, unknown>): Promise<void> {
  return new Promise((resolveDone) => {
    const sessionID = String(payload.session_id ?? payload.sessionID ?? "")
    try {
      log(`fire event=${event} session_id=${sessionID} hook=${HOOK} python=${PYTHON} ${envSummary()}`)
      const child = spawn(PYTHON, [HOOK, event], {
        stdio: ["pipe", "ignore", "ignore"],
        env: process.env,
      })
      const timeout = setTimeout(() => {
        log(`hook timeout event=${event} session_id=${sessionID}`, true)
        child.kill("SIGTERM")
        resolveDone()
      }, 30000)
      child.on("error", (error) => {
        clearTimeout(timeout)
        log(`spawn error event=${event} error=${error instanceof Error ? error.message : String(error)}`, true)
        resolveDone()
      })
      child.on("close", (code, signal) => {
        clearTimeout(timeout)
        log(`hook closed event=${event} session_id=${sessionID} code=${code ?? ""} signal=${signal ?? ""}`)
        resolveDone()
      })
      child.stdin?.on("error", (error) => {
        log(`stdin error event=${event} error=${error instanceof Error ? error.message : String(error)}`, true)
      })
      child.stdin?.end(JSON.stringify({ event, ...payload }))
    } catch (error) {
      log(`fire exception event=${event} error=${error instanceof Error ? error.message : String(error)}`, true)
      resolveDone()
    }
  })
}

// Plugin entry. The plugin context does not carry a session id — sessionID
// comes from each individual event/hook input.
export default async function OpikTracePlugin(_ctx: unknown): Promise<unknown> {
  log(`plugin loaded hook=${HOOK} python=${PYTHON} ${envSummary()}`, true)

  // Track which sessions we've seen a `user_prompt` for, so we only fire the
  // start event once per session even if message.updated fires repeatedly.
  const userPromptSeen = new Set<string>()

  // NOTE: hooks live at the top level of the returned object, NOT inside a
  // `hooks` wrapper. The DESIGN doc's `{ hooks: {...} }` sketch is wrong against
  // real opencode — wrapping causes every hook to be silently ignored.
  return {
    "tool.execute.after": async (
      input: { sessionID?: string; tool?: string; callID?: string },
      _output: unknown,
    ) => {
      const sessionID = input.sessionID
      if (!sessionID) return
      log(`tool.execute.after session_id=${sessionID} tool=${input.tool ?? ""}`)
      await fire("tool_complete", {
        session_id: sessionID,
        tool: input.tool,
        call_id: input.callID,
      })
    },

    "experimental.session.compacting": async (
      input: { sessionID?: string },
      _output: unknown,
    ) => {
      const sessionID = input?.sessionID
      if (!sessionID) return
      log(`experimental.session.compacting session_id=${sessionID}`)
      fire("session_compacting", { session_id: sessionID })
    },

    event: async ({ event }: { event: { type: string; properties?: Record<string, unknown> } }) => {
      const props = (event.properties ?? {}) as Record<string, unknown>

      if (event.type === "message.updated") {
        const message = (props.info ?? props.message) as { sessionID?: string; role?: string } | undefined
        const sessionID = message?.sessionID ?? (props.sessionID as string | undefined)
        if (!sessionID) {
          log(`message.updated ignored missing sessionID keys=${Object.keys(props).join(",")}`)
          return
        }
        if (message?.role !== "user") return
        if (userPromptSeen.has(sessionID)) {
          // Re-fire is fine (Python is idempotent), but cheap to skip.
          return
        }
        userPromptSeen.add(sessionID)
        log(`message.updated user session_id=${sessionID}`)
        await fire("user_prompt", { session_id: sessionID })
        return
      }

      if (event.type === "session.idle") {
        // Per-turn idle: opencode fires this every time the LLM stops between
        // turns. Must NOT map to "session_end" — that would finalize the trace
        // and the Python hook's trace_finalized gate would drop every event
        // after turn 1 (including the next turn's user_prompt).
        const sessionID = (props.sessionID ?? props.session_id) as string | undefined
        if (!sessionID) {
          log(`${event.type} ignored missing sessionID keys=${Object.keys(props).join(",")}`)
          return
        }
        userPromptSeen.delete(sessionID)
        log(`${event.type} session_id=${sessionID}`)
        await fire("session_idle", {
          session_id: sessionID,
          event_timestamp: new Date().toISOString(),
          opencode_event_type: event.type,
        })
        return
      }

      if (event.type === "session.completed") {
        const sessionID = (props.sessionID ?? props.session_id) as string | undefined
        if (!sessionID) {
          log(`${event.type} ignored missing sessionID keys=${Object.keys(props).join(",")}`)
          return
        }
        userPromptSeen.delete(sessionID)
        log(`${event.type} session_id=${sessionID}`)
        await fire("session_end", {
          session_id: sessionID,
          event_timestamp: new Date().toISOString(),
          opencode_event_type: event.type,
        })
        return
      }

      if (event.type === "session.deleted") {
        const sessionID = (props.sessionID ?? props.session_id) as string | undefined
        if (sessionID) userPromptSeen.delete(sessionID)
      }
    },
  }
}
