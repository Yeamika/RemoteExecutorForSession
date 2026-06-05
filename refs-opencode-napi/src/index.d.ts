/**
 * Type declarations for @opencode-ai/refs-opencode native addon.
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

export function createSessionMcp(dbPath: string, sessionId: string, workdir: string): SessionMcpHandle
export function defaultDbPath(): string
export function getSdkToolDefinitions(handle: SessionMcpHandle): ToolDefinition[]
export function parseToolCallResult(json: string): {
  error?: { code: number; message: string }
  result?: ToolCallResult
}
export function extractOutputText(json: string): string
