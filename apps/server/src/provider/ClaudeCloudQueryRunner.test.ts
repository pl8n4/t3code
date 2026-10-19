import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeVM from "node:vm";
import { describe, expect, it } from "@effect/vitest";
import {
  ClaudeSettings,
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  PreviewAutomationNoAvailableHostError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PreviewAutomationBroker from "../mcp/PreviewAutomationBroker.ts";
import * as ProcessRunner from "../processRunner.ts";
import { makeClaudeCloudQueryRunner } from "./ClaudeCloudQueryRunner.ts";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeSettings = Schema.decodeEffect(ClaudeSettings);
const environmentId = EnvironmentId.make("cloud-test-environment");
const threadId = ThreadId.make("cloud-test-thread");
const funded = () => ({
  extra_usage: { is_enabled: false },
  spend: { enabled: false },
  iguana_necktie: {
    limit_dollars: 100,
    remaining_dollars: 99.8,
    resets_at: "2999-01-01T00:00:00Z",
    locked_reason: null,
  },
});

function fixture(input?: {
  paid?: boolean;
  model?: string;
  createFails?: boolean;
  events?: unknown[];
  workspace?: boolean;
  workspaceUnavailable?: boolean;
  workspaceMalformed?: boolean;
  workspaceCwd?: string;
  setupFails?: boolean;
  initializationEvents?: unknown[];
}) {
  const workspaceEntered = Promise.withResolvers<void>();
  const workspaceRelease = Promise.withResolvers<void>();
  const recoveryEntered = Promise.withResolvers<void>();
  const recoveryRelease = Promise.withResolvers<void>();
  let holdWorkspace = false;
  let holdRecovery = false;
  let usage: unknown = input?.paid ? { ...funded(), extra_usage: { is_enabled: true } } : funded();
  const calls: Array<{ path: string; method: string; body?: unknown }> = [];
  let sent = false;
  const rpcCalls: Array<Record<string, unknown>> = [];
  const evaluations: string[] = [];
  const controlReplies: unknown[] = [];
  let sequence = 0;
  let setupFailures = input?.setupFails ? 1 : 0;
  let eventPolls = 0;
  let toolReply = Promise.withResolvers<void>();
  const toolResponses: unknown[] = [];
  const fetch = async (url: string, init?: RequestInit) => {
    const method = init?.method ?? "GET";
    const body = typeof init?.body === "string" ? decodeJson(init.body) : undefined;
    calls.push({ path: url, method, body });
    if (url.endsWith("/usage")) return Response.json(usage);
    if (url.endsWith("/environments"))
      return Response.json({
        environments: [
          { kind: "anthropic_cloud", state: "active", name: "Default", environment_id: "env_test" },
        ],
      });
    if (url.endsWith("/events")) {
      if (method === "GET") {
        eventPolls++;
        return Response.json({ data: [...controlReplies, ...(sent ? (input?.events ?? []) : [])] });
      }
      if (input?.workspace) {
        const payload = (
          body as {
            events: Array<{
              payload: { type: string; request_id: string; request: { subtype: string } };
            }>;
          }
        ).events[0]?.payload;
        if (payload?.type === "control_request" && payload.request.subtype === "mcp_set_servers")
          for (const event of input.initializationEvents ?? [])
            controlReplies.push({
              event_id: `initialization:${++sequence}`,
              sequence_num: String(sequence),
              payload: event,
            });
        if (payload?.type === "control_request")
          controlReplies.push({
            event_id: `reply:${++sequence}`,
            sequence_num: String(sequence),
            payload: {
              type: "control_response",
              response: {
                subtype: "success",
                request_id: payload.request_id,
                response:
                  payload.request.subtype === "mcp_set_servers" && setupFailures-- > 0
                    ? { errors: { "t3-code": "injected failure" } }
                    : payload.request.subtype === "mcp_status"
                      ? {
                          mcpServers: [
                            {
                              name: "t3-code",
                              status: "connected",
                              source: "sdk",
                              config: { type: "sdk", name: "t3-code" },
                            },
                          ],
                        }
                      : {},
              },
            },
          });
        if (payload?.type === "control_response") {
          toolResponses.push(payload);
          toolReply.resolve();
        }
      }
      if (
        (body as { events?: { payload: { type: string } }[] })?.events?.some(
          (e) => e.payload.type === "user",
        )
      )
        sent = true;
      return Response.json({});
    }
    if (url === "/v1/code/sessions" && input?.createFails)
      return Response.json({}, { status: 503 });
    if (url === "/v1/code/sessions")
      return Response.json({
        deduplicated: false,
        session: { id: "cse_test", config: { model: "claude-haiku-5-5" } },
      });
    return Response.json({
      response_shape: { id: "cse_test", config: { model: "claude-haiku-5-5" } },
    });
  };
  const context = NodeVM.createContext({
    fetch,
    location: { origin: "https://claude.ai", pathname: "/code" },
    AbortSignal,
    TextEncoder,
    btoa,
  });
  const broker = PreviewAutomationBroker.PreviewAutomationBroker.of({
    connect: () => Effect.succeed(Stream.empty),
    focusHost: () => Effect.void,
    respond: () => Effect.void,
    invoke: <A>(request: PreviewAutomationBroker.PreviewAutomationInvokeInput) => {
      if (request.operation === "open")
        return Effect.succeed({ tabId: PreviewTabId.make("cloud-test-tab") } as A);
      if (request.operation === "navigate") {
        recoveryEntered.resolve();
        return holdRecovery
          ? Effect.promise(() => recoveryRelease.promise).pipe(Effect.as({} as A))
          : Effect.succeed({} as A);
      }
      const expression = (request.input as { expression: string }).expression;
      evaluations.push(expression);
      return Effect.tryPromise({
        try: async () => {
          const value = await NodeVM.runInContext(expression, context);
          if (
            expression.length > 64000 ||
            Buffer.byteLength(JSON.stringify(value) ?? "null") > 64000
          )
            throw new Error("Browser evaluation exceeds its real limit");
          return value as A;
        },
        catch: (cause) =>
          new PreviewAutomationNoAvailableHostError({
            operation: request.operation,
            environmentId,
            threadId,
            providerSessionId: "test",
            providerInstanceId: ProviderInstanceId.make("cloud-test"),
            cause,
          }),
      });
    },
  });
  const localClient = HttpClient.make((request) =>
    Effect.gen(function* () {
      if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON");
      const rpc = decodeJson(new TextDecoder().decode(request.body.body)) as Record<
        string,
        unknown
      >;
      rpcCalls.push(rpc);
      if (
        holdWorkspace &&
        rpc.method === "tools/call" &&
        (rpc.params as { name?: string })?.name === "t3_workspace_info"
      ) {
        workspaceEntered.resolve();
        yield* Effect.promise(() => workspaceRelease.promise);
      }
      return HttpClientResponse.fromWeb(
        request,
        rpc.method === "notifications/initialized"
          ? new Response(null, { status: 202 })
          : Response.json({
              jsonrpc: "2.0",
              id: rpc.id,
              result:
                rpc.method === "tools/call"
                  ? {
                      isError: input?.workspaceUnavailable ?? false,
                      content: [
                        {
                          type: "text",
                          text: input?.workspaceMalformed
                            ? "unavailable"
                            : json({
                                projectId: "project-football",
                                projectName: "football-data",
                                cwd: input?.workspaceCwd ?? "/projects/football-data/worktree",
                                workspaceRoot: "/projects/football-data",
                                branch: "test-branch",
                                repository: "github.com/pl8n4/football-data",
                              }),
                        },
                      ],
                    }
                  : {
                      protocolVersion: "2025-06-18",
                      capabilities: {},
                      serverInfo: { name: "T3", version: "1" },
                    },
            }),
      );
    }),
  );
  const setup = Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped();
    yield* fs.writeFileString(
      `${dir}/.claude.json`,
      json({ oauthAccount: { organizationUuid: "org_test" } }),
    );
    const config = yield* decodeSettings({
      homePath: dir,
      cloudFreeCreditsOnly: true,
    });
    const runner = yield* makeClaudeCloudQueryRunner(
      config,
      {},
      ProviderInstanceId.make("cloud-test"),
    ).pipe(
      Effect.provideService(PreviewAutomationBroker.PreviewAutomationBroker, broker),
      Effect.provideService(ServerEnvironment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(environmentId),
      }),
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: () => Effect.die("No process should run without a repository"),
      }),
      Effect.provide(ServerConfig.ServerConfig.layerTest(dir, `${dir}/state`)),
    );
    const sessionId = yield* runner.allocateSessionId;
    const bindingPath = `${dir}/state/userdata/claude-cloud-sessions/${sessionId}.json`;
    const open = (options?: { resume?: boolean; durableCloudSequence?: string }) =>
      runner
        .open({
          threadId,
          providerSessionId: ProviderSessionId.make("cloud-test-session"),
          ...(options?.durableCloudSequence === undefined
            ? {}
            : { durableCloudSequence: options.durableCloudSequence }),
          options: {
            ...(options?.resume ? { resume: sessionId } : { sessionId }),
            model: input?.model ?? "claude-haiku-5-5",
            tools: [],
            permissionMode: "bypassPermissions",
            ...(input?.workspace
              ? {
                  cwd: "/projects/football-data/worktree",
                  mcpServers: {
                    "t3-code": {
                      type: "http" as const,
                      url: "http://127.0.0.1/mcp",
                      headers: { Authorization: "Bearer local-only" },
                    },
                  },
                }
              : {}),
          },
        })
        .pipe(Effect.provideService(HttpClient.HttpClient, localClient));
    return {
      holdWorkspace: () => {
        holdWorkspace = true;
      },
      waitWorkspace: Effect.promise(() => workspaceEntered.promise),
      releaseWorkspace: () => workspaceRelease.resolve(),
      holdRecovery: () => {
        holdRecovery = true;
      },
      waitRecovery: Effect.promise(() => recoveryEntered.promise),
      releaseRecovery: () => recoveryRelease.resolve(),
      runner,
      open,
      calls,
      rpcCalls,
      evaluations,
      readBinding: fs.readFileString(bindingPath).pipe(Effect.map(decodeJson)),
      toolResponses,
      waitForToolReply: Effect.promise(() => toolReply.promise).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            toolReply = Promise.withResolvers<void>();
          }),
        ),
      ),
      eventPolls: () => eventPolls,
      pushEvent: (payload: unknown) =>
        controlReplies.push({
          event_id: `injected:${++sequence}`,
          sequence_num: String(sequence),
          payload,
        }),
      replayToolRequest: () => {
        const previous = controlReplies.find(
          (e) => (e as { payload: { type: string } }).payload.type === "control_request",
        ) as { payload: unknown };
        controlReplies.push({
          event_id: `duplicate:${++sequence}`,
          sequence_num: String(sequence),
          payload: previous.payload,
        });
      },
      forgetToolOutcome: (requestId: string) =>
        fs.readFileString(bindingPath).pipe(
          Effect.map(
            Schema.decodeUnknownSync(
              Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
            ),
          ),
          Effect.flatMap((saved) =>
            fs.writeFileString(
              bindingPath,
              json({ ...saved, toolRequests: { [requestId]: { state: "started" } } }),
            ),
          ),
        ),
      setPaid: () => {
        usage = { ...funded(), spend: { enabled: true } };
      },
    };
  });
  return setup;
}

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | import("effect/Scope").Scope
    | import("effect/FileSystem").FileSystem
    | import("effect/Path").Path
    | import("effect/Crypto").Crypto
  >,
) => effect.pipe(Effect.scoped, Effect.provide(NodeServices.layer));

describe("Claude cloud guarded transport", () => {
  it.effect.each(["tool", "reply"] as const)(
    "blocks a new prompt while late %s recovery is awaiting browser navigation",
    (kind) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Completed task" },
          });
          f.pushEvent({ type: "result", subtype: "success", result: "Done" });
          yield* TestClock.adjust("200 millis");
          const result = yield* session.messages.pipe(
            Stream.filter((m) => m.type === "result"),
            Stream.runHead,
          );
          if (result._tag !== "Some") throw new Error("Missing result");
          const checkpoint = session.cloudEventSequence?.(result.value);
          if (checkpoint === undefined) throw new Error("Missing checkpoint");
          f.holdWorkspace();
          f.holdRecovery();
          const offer = yield* session
            .offer(
              {
                type: "user",
                session_id: "",
                parent_tool_use_id: null,
                message: { role: "user", content: "Must not send during recovery" },
              },
              checkpoint,
            )
            .pipe(Effect.result, Effect.forkChild);
          yield* f.waitWorkspace;
          f.pushEvent(
            kind === "tool"
              ? {
                  type: "control_request",
                  request_id: "recovery-write",
                  request: {
                    subtype: "mcp_message",
                    server_name: "t3-code",
                    message: {
                      jsonrpc: "2.0",
                      id: 204,
                      method: "tools/call",
                      params: { name: "t3_workspace_write", arguments: {} },
                    },
                  },
                }
              : {
                  type: "assistant",
                  message: { content: [{ type: "text", text: "Late output" }] },
                },
          );
          yield* TestClock.adjust("2 seconds");
          yield* f.waitRecovery;
          f.releaseWorkspace();
          const offered = yield* Fiber.join(offer);
          const subsequent = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Must also refuse" },
            })
            .pipe(Effect.result);
          f.releaseRecovery();
          yield* session.close;
          expect(offered._tag).toBe("Failure");
          expect(JSON.stringify(offered)).toContain("delivery_pending");
          expect(subsequent._tag).toBe("Failure");
          expect(JSON.stringify(subsequent)).toContain("delivery_pending");
          expect(
            f.calls
              .flatMap(
                (c) => (c.body as { events?: Array<{ payload: { type: string } }> })?.events ?? [],
              )
              .filter((e) => e.payload.type === "user"),
          ).toHaveLength(1);
          expect(
            f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
          ).toHaveLength(0);
        }),
      ),
  );
  it.effect("stops initialization when a late tool request arrives during MCP setup", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({
          workspace: true,
          initializationEvents: [
            {
              type: "control_request",
              request_id: "setup-write",
              request: {
                subtype: "mcp_message",
                server_name: "t3-code",
                message: {
                  jsonrpc: "2.0",
                  id: 203,
                  method: "tools/call",
                  params: { name: "t3_workspace_write", arguments: {} },
                },
              },
            },
          ],
        });
        const opened = yield* f.open().pipe(Effect.result);
        expect(opened._tag).toBe("Failure");
        expect(JSON.stringify(opened)).toContain("delivery_pending");
        expect(
          f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
        ).toHaveLength(0);
      }),
    ),
  );
  it.effect(
    "blocks unfetched late output and tools after disconnecting before the first reply",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Original task" },
          });
          expect(yield* f.readBinding).toMatchObject({ pendingTurn: {} });
          yield* session.close;
          f.pushEvent({
            type: "control_request",
            request_id: "late-write",
            request: {
              subtype: "mcp_message",
              server_name: "t3-code",
              message: {
                jsonrpc: "2.0",
                id: 200,
                method: "tools/call",
                params: { name: "t3_workspace_write", arguments: {} },
              },
            },
          });
          f.pushEvent({
            type: "assistant",
            message: { content: [{ type: "text", text: "Late output" }] },
          });
          f.pushEvent({ type: "result", subtype: "success", result: "Late output" });
          const resumed = yield* f
            .open({ resume: true, durableCloudSequence: "0" })
            .pipe(Effect.result);
          expect(resumed._tag).toBe("Failure");
          expect(JSON.stringify(resumed)).toContain("delivery_pending");
          expect(
            f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
          ).toHaveLength(0);
          expect(JSON.stringify(f.calls)).not.toContain("New task");
          const closedOffer = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "New task" },
            })
            .pipe(Effect.result);
          expect(closedOffer._tag).toBe("Failure");
          expect(
            f.calls
              .flatMap(
                (c) => (c.body as { events?: Array<{ payload: { type: string } }> })?.events ?? [],
              )
              .filter((e) => e.payload.type === "user"),
          ).toHaveLength(1);
        }),
      ),
  );
  it.effect.each(["tool", "reply"] as const)(
    "refuses newly fetched historical %s events during initialization",
    (first) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          yield* session.close;
          const tool = {
            type: "control_request",
            request_id: "historical-write",
            request: {
              subtype: "mcp_message",
              server_name: "t3-code",
              message: {
                jsonrpc: "2.0",
                id: 201,
                method: "tools/call",
                params: { name: "t3_workspace_write", arguments: {} },
              },
            },
          };
          const reply = {
            type: "assistant",
            message: { content: [{ type: "text", text: "Historical output" }] },
          };
          for (const payload of first === "tool" ? [tool, reply] : [reply, tool])
            f.pushEvent(payload);
          const resumed = yield* f.open({ resume: true }).pipe(Effect.result);
          expect(resumed._tag).toBe("Failure");
          expect(JSON.stringify(resumed)).toContain("delivery_pending");
          expect(
            f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
          ).toHaveLength(0);
          expect(yield* f.readBinding).toMatchObject({
            inbox: expect.arrayContaining([expect.objectContaining({ payload: reply })]),
          });
        }),
      ),
  );
  it.effect(
    "rejects idle tool requests arriving after initialization and refuses the next prompt",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          f.pushEvent({
            type: "control_request",
            request_id: "idle-write",
            request: {
              subtype: "mcp_message",
              server_name: "t3-code",
              message: {
                jsonrpc: "2.0",
                id: 202,
                method: "tools/call",
                params: { name: "t3_workspace_write", arguments: {} },
              },
            },
          });
          yield* TestClock.adjust("2 seconds");
          const output = yield* session.messages.pipe(Stream.runHead, Effect.result);
          expect(output._tag).toBe("Failure");
          expect(JSON.stringify(output)).toContain("delivery_pending");
          const offered = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Must not send" },
            })
            .pipe(Effect.result);
          expect(offered._tag).toBe("Failure");
          expect(JSON.stringify(offered)).toContain("delivery_pending");
          expect(
            f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
          ).toHaveLength(0);
          expect(JSON.stringify(f.calls)).not.toContain("Must not send");
          yield* session.close;
        }),
      ),
  );
  it.effect("refuses a missing explicit resume without creating a replacement session", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture();
        const result = yield* f.open({ resume: true }).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        expect(JSON.stringify(result)).toContain("session_missing");
        expect(f.calls.some((call) => call.method === "POST")).toBe(false);
      }),
    ),
  );
  it.effect("closes a failed setup's poller and does not repeat a cached tool on retry", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true, setupFails: true });
        expect((yield* f.open().pipe(Effect.result))._tag).toBe("Failure");
        const polls = f.eventPolls();
        yield* TestClock.adjust("10 seconds");
        expect(f.eventPolls()).toBe(polls);
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Continue" },
        });
        f.pushEvent({
          type: "control_request",
          request_id: "write-once",
          request: {
            subtype: "mcp_message",
            server_name: "t3-code",
            message: {
              jsonrpc: "2.0",
              id: 100,
              method: "tools/call",
              params: { name: "t3_workspace_write", arguments: {} },
            },
          },
        });
        yield* TestClock.adjust("200 millis");
        yield* f.waitForToolReply;
        expect(
          f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
        ).toHaveLength(1);
        f.pushEvent({ type: "result", subtype: "success", result: "Done" });
        yield* TestClock.adjust("200 millis");
        const result = yield* session.messages.pipe(
          Stream.filter((m) => m.type === "result"),
          Stream.runHead,
        );
        if (result._tag !== "Some") throw new Error("Missing result");
        const checkpoint = session.cloudEventSequence?.(result.value);
        if (checkpoint === undefined) throw new Error("Missing checkpoint");
        yield* session.close;
        f.replayToolRequest();
        const resumed = yield* f.open({ resume: true, durableCloudSequence: checkpoint });
        yield* f.waitForToolReply;
        yield* resumed.close;
        expect(
          f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
        ).toHaveLength(1);
        expect(f.toolResponses.length).toBeGreaterThanOrEqual(2);
      }),
    ),
  );
  it.effect("does not re-execute a tool whose persisted outcome is uncertain", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true });
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Continue" },
        });
        f.pushEvent({
          type: "control_request",
          request_id: "uncertain-write",
          request: {
            subtype: "mcp_message",
            server_name: "t3-code",
            message: {
              jsonrpc: "2.0",
              id: 101,
              method: "tools/call",
              params: { name: "t3_workspace_write", arguments: {} },
            },
          },
        });
        yield* TestClock.adjust("200 millis");
        yield* f.waitForToolReply;
        f.pushEvent({ type: "result", subtype: "success", result: "Done" });
        yield* TestClock.adjust("200 millis");
        const result = yield* session.messages.pipe(
          Stream.filter((m) => m.type === "result"),
          Stream.runHead,
        );
        if (result._tag !== "Some") throw new Error("Missing result");
        const checkpoint = session.cloudEventSequence?.(result.value);
        if (checkpoint === undefined) throw new Error("Missing checkpoint");
        yield* session.close;
        yield* f.forgetToolOutcome("uncertain-write");
        f.replayToolRequest();
        const resumed = yield* f.open({ resume: true, durableCloudSequence: checkpoint });
        yield* f.waitForToolReply;
        yield* resumed.close;
        expect(
          f.rpcCalls.filter((rpc) => JSON.stringify(rpc).includes("t3_workspace_write")),
        ).toHaveLength(1);
        expect(JSON.stringify(f.toolResponses)).toContain("It was not repeated");
      }),
    ),
  );
  it.effect(
    "preserves unread replies and blocks unconfirmed delivery instead of skipping it on resume",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({
            events: [
              {
                event_id: "first",
                sequence_num: "1",
                payload: {
                  type: "assistant",
                  message: { content: [{ type: "text", text: "first reply" }] },
                },
              },
              {
                event_id: "unread",
                sequence_num: "2",
                payload: {
                  type: "assistant",
                  message: { content: [{ type: "text", text: "unread reply" }] },
                },
              },
            ],
          });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Read" },
          });
          yield* session.messages.pipe(Stream.take(1), Stream.runDrain);
          yield* session.close;
          const resumed = yield* f
            .open({ resume: true, durableCloudSequence: "1" })
            .pipe(Effect.result);
          expect(resumed._tag).toBe("Failure");
          expect(JSON.stringify(resumed)).toContain("delivery_pending");
          expect(yield* f.readBinding).toMatchObject({
            inbox: expect.arrayContaining([
              { event_id: "unread", sequence_num: "2", payload: expect.anything() },
            ]),
          });
          expect(
            f.calls.filter((call) => call.path === "/v1/code/sessions" && call.method === "POST"),
          ).toHaveLength(1);
        }),
      ),
  );
  it.effect(
    "supplies project and PR tools for an ordinary question without special user prompting",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: {
              role: "user",
              content: "Can you tell me about recent PRs regarding documentation?",
            },
          });
          const user = f.calls
            .flatMap(
              (c) =>
                (
                  c.body as {
                    events?: Array<{ payload: { type: string; message?: { content: unknown } } }>;
                  }
                )?.events ?? [],
            )
            .find((e) => e.payload.type === "user");
          const text = JSON.stringify(user);
          expect(text).toContain("Can you tell me about recent PRs regarding documentation?");
          expect(text).toContain("football-data");
          expect(text).toContain("t3_workspace_pull_requests");
          expect(text).toContain("/projects/football-data/worktree");
          expect(text).not.toContain("local-only");
          expect(f.rpcCalls.filter((c) => c.method === "tools/call")).toHaveLength(1);
          expect(
            f.evaluations.every(
              (e) =>
                !e.includes(".push(") &&
                !e.startsWith("btoa(") &&
                !e.startsWith("delete globalThis"),
            ),
          ).toBe(true);
          yield* session.close;
        }),
      ),
  );
  it.effect.each(["error", "malformed", "different"] as const)(
    "does not send the task if workspace verification returns %s",
    (kind) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({
            workspace: true,
            workspaceUnavailable: kind === "error",
            workspaceMalformed: kind === "malformed",
            ...(kind === "different" ? { workspaceCwd: "/projects/unrelated" } : {}),
          });
          const session = yield* f.open();
          const result = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Inspect my repo" },
            })
            .pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          expect(JSON.stringify(f.calls)).not.toContain("Inspect my repo");
          yield* session.close;
        }),
      ),
  );
  it.effect("never creates a session when paid usage is enabled", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ paid: true });
        const result = yield* f.open().pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        expect(f.calls.some((c) => c.method === "POST")).toBe(false);
      }),
    ),
  );
  it.effect("rejects expensive models before even opening the browser", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ model: "claude-opus-5-5" });
        const result = yield* f.open().pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        expect(f.calls).toEqual([]);
      }),
    ),
  );
  it.effect("creates an empty Haiku session and checks billing again before each message", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture();
        const session = yield* f.open();
        expect(f.calls.find((c) => c.path === "/v1/code/sessions")?.body).toMatchObject({
          config: { model: "claude-haiku-5-5" },
          events: [],
        });
        f.setPaid();
        const result = yield* session
          .offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Never send this" },
          })
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        expect(f.calls.some((c) => c.path.endsWith("/events") && c.method === "POST")).toBe(false);
        yield* session.close;
      }),
    ),
  );
  it.effect("does not duplicate an uncertain session creation after retry", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ createFails: true });
        yield* f.open().pipe(Effect.result);
        yield* f.open().pipe(Effect.result);
        expect(
          f.calls.filter((c) => c.path === "/v1/code/sessions" && c.method === "POST"),
        ).toHaveLength(1);
      }),
    ),
  );
  it.effect("delivers cloud replies in order and resumes the same remote session", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({
          events: [
            {
              event_id: "result",
              sequence_num: "3",
              payload: { type: "result", session_id: "remote", subtype: "success", result: "OK" },
            },
            {
              event_id: "init",
              sequence_num: "2",
              payload: { type: "system", subtype: "init", secret: "must stay in browser" },
            },
            {
              event_id: "assistant",
              sequence_num: "1",
              payload: {
                type: "assistant",
                session_id: "remote",
                message: { content: [{ type: "text", text: "OK" }] },
              },
            },
          ],
        });
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Reply OK" },
        });
        const replies = yield* session.messages.pipe(Stream.take(2), Stream.runCollect);
        expect(replies.map((m) => m.type)).toEqual(["assistant", "result"]);
        expect(replies.every((m) => m.session_id !== "remote")).toBe(true);
        yield* session.close;
        const checkpoint = session.cloudEventSequence?.(replies.at(-1)!);
        if (checkpoint === undefined) throw new Error("Missing cloud delivery checkpoint");
        const resumed = yield* f.open({
          resume: true,
          durableCloudSequence: checkpoint,
        });
        expect(
          f.calls.filter((c) => c.path === "/v1/code/sessions" && c.method === "POST"),
        ).toHaveLength(1);
        yield* resumed.close;
      }),
    ),
  );
  it.effect("interrupts remote work when the T3 session closes", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture();
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Work" },
        });
        yield* session.close;
        const posts = f.calls.filter((c) => c.path.endsWith("/events") && c.method === "POST");
        expect(posts.at(0)?.body).toMatchObject({
          events: [
            { payload: { request: { subtype: "set_permission_mode", mode: "bypassPermissions" } } },
          ],
        });
        expect(posts.at(-1)?.body).toMatchObject({
          events: [{ payload: { request: { subtype: "interrupt" } } }],
        });
      }),
    ),
  );
  it.effect("transfers event payloads above the browser result limit without losing Unicode", () =>
    run(
      Effect.gen(function* () {
        const text = "漢字🙂".repeat(24000);
        const f = yield* fixture({
          events: [
            {
              event_id: "large",
              sequence_num: "1",
              payload: {
                type: "assistant",
                session_id: "remote",
                message: { content: [{ type: "text", text }] },
              },
            },
          ],
        });
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Read large output" },
        });
        const reply = yield* session.messages.pipe(Stream.runHead);
        expect(reply._tag).toBe("Some");
        if (reply._tag === "Some" && reply.value.type === "assistant")
          expect(reply.value.message.content).toEqual([{ type: "text", text }]);
        yield* session.close;
      }),
    ),
  );
});
