import { OrchestratorMcpFailure } from "@t3tools/contracts";
import type { Options } from "@anthropic-ai/claude-agent-sdk";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest } from "effect/http";

import { isClaudeCloudToolAllowed } from "./ClaudeCloudAllowedTools.ts";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const tryJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const Rpc = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
  method: Schema.optionalKey(Schema.String),
  params: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
});
const decodeRpc = Schema.decodeUnknownEffect(Rpc);

// The local credential never crosses the cloud/browser boundary. The remote CLI sees an SDK
// server; its JSON-RPC messages use T3's ordinary authenticated MCP endpoint on this machine.
export const makeClaudeCloudMcpRelay = Effect.fn("makeClaudeCloudMcpRelay")(function* (
  servers: Options["mcpServers"],
) {
  const server = servers?.["t3-code"];
  const maybeClient = yield* Effect.serviceOption(HttpClient.HttpClient);
  if (!server || server.type !== "http" || Option.isNone(maybeClient)) return undefined;
  const endpoint = yield* Effect.try(() => new URL(server.url));
  if (
    !["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname) ||
    endpoint.protocol !== "http:"
  )
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "The cloud bridge requires T3's local MCP endpoint.",
    });
  const client = maybeClient.value;
  let sessionId: string | undefined;
  const request = Effect.fn("ClaudeCloudMcpRelay.request")(function* (message: unknown) {
    const rpc = yield* decodeRpc(message);
    const response = yield* HttpClientRequest.post(server.url).pipe(
      HttpClientRequest.setHeaders({
        ...server.headers,
        accept: "application/json, text/event-stream",
        ...(sessionId ? { "mcp-session-id": sessionId } : {}),
      }),
      HttpClientRequest.bodyText(json(message), "application/json"),
      client.execute,
    );
    sessionId = response.headers["mcp-session-id"] ?? sessionId;
    if (response.status < 200 || response.status >= 300)
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The local T3 tool connection is unavailable. Stop and retry after reconnecting.",
      });
    if (rpc.id === undefined || response.status === 202)
      return { jsonrpc: "2.0", id: rpc.id ?? 0, result: {} };
    if (response.headers["content-type"]?.includes("text/event-stream")) {
      const reply = yield* response.stream.pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.filter((line) => line.startsWith("data:")),
        Stream.map((line) => tryJson(line.slice(5).trim())),
        Stream.filter(Option.isSome),
        Stream.map((option) => option.value),
        Stream.filter(
          (value) =>
            typeof value === "object" && value !== null && "id" in value && value.id === rpc.id,
        ),
        Stream.runHead,
      );
      return yield* Effect.fromOption(reply);
    }
    return yield* response.text.pipe(Effect.map(decodeJson));
  });
  const initialized = yield* request({
    jsonrpc: "2.0",
    id: -1,
    method: "initialize",
    params: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "T3 cloud relay", version: "1" },
    },
  });
  yield* request({ jsonrpc: "2.0", method: "notifications/initialized" });
  return Effect.fn("ClaudeCloudMcpRelay.relay")(function* (message: unknown) {
    const rpc = yield* decodeRpc(message);
    if (rpc.method === "initialize" && typeof initialized === "object" && initialized !== null)
      return { ...initialized, id: rpc.id };
    if (rpc.method === "notifications/initialized")
      return { jsonrpc: "2.0", id: rpc.id ?? 0, result: {} };
    if (rpc.method === "tools/call" && !isClaudeCloudToolAllowed(String(rpc.params?.name)))
      return {
        jsonrpc: "2.0",
        id: rpc.id,
        result: {
          isError: true,
          content: [
            {
              type: "text",
              text: "This tool is unavailable in the free-credit workflow. Use guarded T3 delegation or scheduling tools for model work; provider settings and other launch routes remain unavailable.",
            },
          ],
        },
      };
    const reply = yield* request(message);
    if (
      rpc.method === "tools/list" &&
      typeof reply === "object" &&
      reply !== null &&
      "result" in reply &&
      typeof reply.result === "object" &&
      reply.result !== null &&
      "tools" in reply.result &&
      Array.isArray(reply.result.tools)
    )
      return {
        ...reply,
        result: {
          ...reply.result,
          tools: reply.result.tools.filter(
            (tool) => typeof tool?.name === "string" && isClaudeCloudToolAllowed(tool.name),
          ),
        },
      };
    return reply;
  });
});

export const CLAUDE_CLOUD_WORKSPACE_INSTRUCTIONS = `You are working inside T3 Code. The project and source-control credentials are on the user's machine, connected through the t3-code MCP server. Call t3_workspace_info to identify the actual project and worktree. Use t3_workspace_pull_requests and t3_workspace_pull_request_read for PR questions, with all states to include merged PRs. For topic lists, filter by relevant labels or titles, follow nextCursors when truncated, and never fill a requested count with unrelated PRs. Use t3_workspace_list/read/write for workspace-relative file operations and t3_workspace_exec for git, gh, rg, tests and patches in the actual project. Commands return a persistent jobId; use t3_workspace_job_read with returned byte offsets to wait for completion and collect output, t3_workspace_job_list to find existing jobs, and t3_workspace_job_cancel to cancel. Do not rerun a command just because it is still running or a connection was interrupted. Commands are noninteractive and have no default execution deadline. Cloud Bash, Read, Write, Edit, Glob and Grep are disabled because their /home/user directory is a disposable container unrelated to the project; an empty cloud directory never means the T3 repository is unavailable. Preserve existing edits. Do not clone, commit or push merely to connect the workspace. T3 records local diffs and checkpoints. Link PRs you create or work on with link_pull_request. For browser work, call preview_status first and then preview_open if needed. Use T3 tools for browser, history and user input. This session uses promotional cloud credit and the model explicitly selected in the T3 composer. Never switch your own model, run local agent CLIs, make paid API inference calls or change provider settings. Use delegate_task for subagents, with orchestrator_capabilities to discover targets; native Agent/Task tools are unavailable. Prefer mode=async for long tasks and inspect task_status when their result is needed. Do not use a long synchronous wait that can exceed the cloud tool timeout. Claude children must use the same verified free-credit cloud instance. Use Codex children only when the user explicitly requests Codex delegation; the environment must separately enable subscription Codex delegation and verify its ChatGPT login. Codex uses its separate subscription, never Claude promotional credits. Child tasks inherit the free-credit workflow restrictions. If a child has a pending question, read and answer it with t3_pending_request_list/read/respond for that childThreadId; this cannot approve permission requests. Use T3 schedule_task and watch_pull_request for requested automation; every future model turn rechecks its permitted route and refuses paid or unknown routes. Do not use ordinary top-level thread launch/send or other launch tools as a delegation workaround.`;
