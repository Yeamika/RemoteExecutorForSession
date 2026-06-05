/**
 * REFS-opencode: napi-rs bindings for RemoteExecutorForSession SDK.
 *
 * Provides an embedded MCP handler that reads/writes OpenCode's SQLite database
 * directly, handling hashRef pipeline, Caller routing, and exbash task tracking.
 *
 * Tool schemas are the single source of truth from the Rust SDK's `mcptooldefs.rs`,
 * read dynamically via `tools/list` - never duplicated in TS.
 *
 * Native addon is loaded lazily at runtime - the .node file must be built
 * by napi-rs before use. If not available, functions will throw.
 */

export interface ToolDefinition {
  name: string
  description: string | null
  inputSchema: Record<string, unknown>
}

export interface ToolCallResult {
  content: Array<{ type: string; text: string }>
}

export interface SessionMcpHandle {
  listTools(): string
  callTool(name: string, argsJson: string): string
  callToolText(name: string, argsJson: string): string
  listExecutorsJson(): string
  handleRaw(request: string): string
}

import { existsSync } from "fs"
import { dirname, join } from "path"

// Lazy-loaded native addon reference
let _addon: any = undefined

const bindings: Record<string, string> = {
  "win32-x64": "refs-opencode.win32-x64-msvc.node",
  "win32-arm64": "refs-opencode.win32-arm64-msvc.node",
  "linux-x64": "refs-opencode.linux-x64-gnu.node",
  "linux-arm64": "refs-opencode.linux-arm64-gnu.node",
  "darwin-x64": "refs-opencode.darwin-x64.node",
  "darwin-arm64": "refs-opencode.darwin-arm64.node",
}

function linuxBinding() {
  if (process.platform !== "linux") return
  const isMusl = !process.report?.getReport?.().header.glibcVersionRuntime
  if (process.arch === "x64") return isMusl ? "refs-opencode.linux-x64-musl.node" : "refs-opencode.linux-x64-gnu.node"
  if (process.arch === "arm64") return isMusl ? "refs-opencode.linux-arm64-musl.node" : "refs-opencode.linux-arm64-gnu.node"
}

function candidates(filename: string) {
  const paths = []
  if (process.execPath) paths.push(join(dirname(process.execPath), filename))
  paths.push(join(process.cwd(), filename))
  paths.push(join(import.meta.dirname, "..", filename))
  return paths
}

/**
 * Load the native addon. Throws if not built.
 * The .node file is resolved by Bun/Node at runtime from the package directory.
 */
function getAddon(): any {
  if (_addon) return _addon
  const filename = linuxBinding() ?? bindings[`${process.platform}-${process.arch}`]
  if (!filename) {
    throw new Error(`REFS-opencode native addon is not available for ${process.platform}-${process.arch}.`)
  }
  for (const file of candidates(filename)) {
    if (!existsSync(file)) continue
    _addon = require(file)
    return _addon
  }
  throw new Error(
    `REFS-opencode native addon ${filename} not found next to the opencode binary. Run \`napi build --platform\` in packages/REFS-opencode first.`,
  )
}

/**
 * Create a session MCP handler backed by OpenCode's SQLite database.
 */
export function createSessionMcp(dbPath: string, sessionId: string, workdir: string): SessionMcpHandle {
  return getAddon().createSessionMcp(dbPath, sessionId, workdir)
}

/**
 * Get the default SQLite database path used by OpenCode.
 */
export function defaultDbPath(): string {
  return getAddon().defaultDbPath()
}

/**
 * Get SDK tool definitions by calling tools/list on the MCP handle.
 */
export function getSdkToolDefinitions(handle: SessionMcpHandle): ToolDefinition[] {
  const json = handle.listTools()
  const parsed = JSON.parse(json)
  return parsed?.result?.tools ?? []
}

/**
 * Parse a tool call result from the JSON string returned by callTool().
 */
export function parseToolCallResult(json: string): {
  error?: { code: number; message: string }
  result?: ToolCallResult
} {
  return JSON.parse(json)
}

/**
 * Extract the model-visible output text from a tool call result.
 */
export function extractOutputText(json: string): string {
  const parsed = parseToolCallResult(json)
  return parsed?.result?.content?.[0]?.text ?? ""
}
