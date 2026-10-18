import { OrchestratorMcpFailure } from "@t3tools/contracts";
import type { Options } from "@anthropic-ai/claude-agent-sdk";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest } from "effect/http";

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

// A cloud-credit turn must not start another provider or change its billing route.
const modelLaunchingTools = new Set([
  "delegate_task",
  "create_threads",
  "t3_thread_launch",
  "t3_thread_send",
  "t3_thread_send_attachments",
  "schedule_task",
  "update_scheduled_task",
  "run_scheduled_task_now",
  "t3_thread_fork",
  "t3_thread_merge_back",
  "t3_queue_promote_to_steer",
  "t3_thread_configure",
  "t3_thread_update",
  "t3_environment_preferences_update",
]);

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
    if (rpc.method === "tools/call" && modelLaunchingTools.has(String(rpc.params?.name)))
      return {
        jsonrpc: "2.0",
        id: rpc.id,
        result: {
          isError: true,
          content: [
            {
              type: "text",
              text: "This free-credit session cannot launch other model work or change provider settings. Use the T3 composer to start another verified cloud turn.",
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
            (tool) => typeof tool?.name === "string" && !modelLaunchingTools.has(tool.name),
          ),
        },
      };
    return reply;
  });
});

export const CLAUDE_CLOUD_WORKSPACE_INSTRUCTIONS = `Your source project is the actual workspace of this T3 thread on the user's machine. Cloud Bash, Read, Write, Edit, Glob and Grep operate in a separate disposable cloud directory and cannot modify that project. For every source-file operation use the t3-code MCP tools t3_workspace_list, t3_workspace_read, t3_workspace_write, and t3_workspace_exec. Use workspace-relative paths. The exec tool runs on the user's machine in the actual project directory; use it for rg, git, tests and apply_patch. Preserve all existing user changes. Do not initialize, clone, commit or push merely to make the workspace available: it is already connected. T3 captures local changes and checkpoints. Use T3's other MCP tools for browser, thread history and user input. Never start another model, local agent CLI, paid API call, or delegated task; this session is restricted to promotional cloud credit and Haiku. Tools that launch model work or change provider settings are disabled; the user can start another verified cloud turn in the T3 composer.`;
