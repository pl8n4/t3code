import { describe, it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientResponse } from "effect/http";
import { makeClaudeCloudMcpRelay } from "./ClaudeCloudMcpRelay.ts";

const decodeJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const servers = {
  "t3-code": {
    type: "http" as const,
    url: "http://127.0.0.1:13773/mcp",
    headers: { Authorization: "Bearer local-only-secret" },
  },
};

describe("cloud MCP protocol relay", () => {
  it.effect("keeps credentials local, negotiates HTTP sessions, and forwards SSE tools", () =>
    Effect.gen(function* () {
      const requests: Array<{
        authorization: string | undefined;
        session: string | undefined;
        method: unknown;
      }> = [];
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON body");
          const rpc = decodeJson(new TextDecoder().decode(request.body.body));
          requests.push({
            authorization: request.headers.authorization,
            session: request.headers["mcp-session-id"],
            method: rpc.method,
          });
          if (rpc.method === "notifications/initialized")
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
          if (rpc.method === "initialize")
            return HttpClientResponse.fromWeb(
              request,
              Response.json(
                {
                  jsonrpc: "2.0",
                  id: rpc.id,
                  result: {
                    protocolVersion: "2025-06-18",
                    capabilities: { tools: {} },
                    serverInfo: { name: "T3 Code", version: "1" },
                  },
                },
                { headers: { "mcp-session-id": "local-session" } },
              ),
            );
          return HttpClientResponse.fromWeb(
            request,
            new Response(
              `event: message\ndata: ${JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result: { tools: [{ name: "t3_workspace_read" }] } })}\n\n`,
              { headers: { "content-type": "text/event-stream" } },
            ),
          );
        }),
      );
      const relay = yield* makeClaudeCloudMcpRelay(servers).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      expect(relay).toBeDefined();
      if (!relay) return;
      const handshake = yield* relay({
        jsonrpc: "2.0",
        id: 22,
        method: "initialize",
        params: { protocolVersion: "2025-11-25" },
      });
      expect(handshake).toMatchObject({ id: 22, result: { protocolVersion: "2025-06-18" } });
      const tools = yield* relay({ jsonrpc: "2.0", id: 23, method: "tools/list" });
      expect(tools).toMatchObject({ id: 23, result: { tools: [{ name: "t3_workspace_read" }] } });
      expect(requests.map((r) => r.method)).toEqual([
        "initialize",
        "notifications/initialized",
        "tools/list",
      ]);
      expect(requests.every((r) => r.authorization === "Bearer local-only-secret")).toBe(true);
      expect(requests[2]?.session).toBe("local-session");
      expect(JSON.stringify([handshake, tools])).not.toContain("local-only-secret");
    }),
  );
  it.effect("hides and blocks tools that could launch billed work or change its provider", () =>
    Effect.gen(function* () {
      let calls = 0;
      let foreignProviderWakeups = 0;
      const client = HttpClient.make((request) => {
        calls++;
        if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON");
        const rpc = decodeJson(new TextDecoder().decode(request.body.body));
        if (
          rpc.method === "tools/call" &&
          (rpc.params as { name?: string })?.name === "watch_pull_request"
        )
          foreignProviderWakeups++;
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            rpc.method === "notifications/initialized"
              ? new Response(null, { status: 202 })
              : Response.json({
                  jsonrpc: "2.0",
                  id: rpc.id,
                  result:
                    rpc.method === "tools/list"
                      ? {
                          tools: [
                            { name: "t3_workspace_exec" },
                            { name: "preview_snapshot" },
                            { name: "delegate_task" },
                            { name: "t3_thread_configure" },
                            { name: "write_pull_request_guides" },
                            { name: "t3_worktree_handoff" },
                            { name: "watch_pull_request" },
                            { name: "t3_pending_request_respond" },
                            { name: "future_model_automation" },
                          ],
                        }
                      : {},
                }),
          ),
        );
      });
      const relay = yield* makeClaudeCloudMcpRelay(servers).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      if (!relay) throw new Error("Missing relay");
      expect(yield* relay({ jsonrpc: "2.0", id: 3, method: "tools/list" })).toMatchObject({
        result: { tools: [{ name: "t3_workspace_exec" }, { name: "preview_snapshot" }] },
      });
      for (const name of [
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
        "write_pull_request_guides",
        "t3_worktree_handoff",
        "watch_pull_request",
        "t3_pending_request_respond",
        "t3_thread_organize",
        "t3_queue_edit",
        "future_model_automation",
      ]) {
        expect(
          yield* relay({
            jsonrpc: "2.0",
            id: 4,
            method: "tools/call",
            params: {
              name,
              arguments: {
                threadId: "foreign-paid-provider-thread",
                url: "https://github.com/example/repo/pull/1",
              },
            },
          }),
        ).toMatchObject({ id: 4, result: { isError: true } });
      }
      expect(calls).toBe(3);
      expect(foreignProviderWakeups).toBe(0);
    }),
  );
  it.effect("never sends the local credential to an external endpoint", () =>
    Effect.gen(function* () {
      let calls = 0;
      const client = HttpClient.make(() => {
        calls++;
        return Effect.die("Must not send");
      });
      const result = yield* makeClaudeCloudMcpRelay({
        "t3-code": { ...servers["t3-code"], url: "https://external.example/mcp" },
      }).pipe(Effect.provideService(HttpClient.HttpClient, client), Effect.result);
      expect(result._tag).toBe("Failure");
      expect(calls).toBe(0);
    }),
  );
  it.effect("does not replay a tool call after an uncertain HTTP failure", () =>
    Effect.gen(function* () {
      let calls = 0;
      const client = HttpClient.make((request) => {
        calls++;
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            calls === 1
              ? Response.json({ jsonrpc: "2.0", id: -1, result: {} })
              : calls === 2
                ? new Response(null, { status: 202 })
                : new Response("Unavailable", { status: 503 }),
          ),
        );
      });
      const relay = yield* makeClaudeCloudMcpRelay(servers).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      if (!relay) throw new Error("Missing relay");
      expect(
        (yield* relay({
          jsonrpc: "2.0",
          id: 1,
          method: "tools/call",
          params: { name: "t3_workspace_write", arguments: {} },
        }).pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(calls).toBe(3);
    }),
  );
});
