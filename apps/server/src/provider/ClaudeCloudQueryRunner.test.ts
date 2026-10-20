import * as NodeServices from "@effect/platform-node/NodeServices";
import type { EffortLevel } from "@anthropic-ai/claude-agent-sdk";
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
import { makeCommandJobs } from "../mcp/AgentWorkspaceCommandJobs.ts";
import { WorkspaceExec } from "../mcp/AgentWorkspaceSchemas.ts";
import { makeClaudeCloudQueryRunner } from "./ClaudeCloudQueryRunner.ts";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeSettings = Schema.decodeEffect(ClaudeSettings);
const decodeWorkspaceExec = Schema.decodeUnknownEffect(WorkspaceExec);
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
  setupStatusLag?: number;
  initializationEvents?: unknown[];
  modelSwitchFails?: boolean;
  effort?: EffortLevel;
  appliedEffortOverride?: EffortLevel;
  reasoningFails?: boolean;
  promptResponseLost?: boolean;
  promptRejectStatus?: number;
  requirePersistentProfile?: boolean;
  rejectReusedTab?: boolean;
  workspaceExec?: (arguments_: unknown) => Effect.Effect<unknown>;
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
  let mcpStatus: "missing" | "connected" | "failed" | "pending" = "missing";
  let reconnectFails = false;
  let holdMcpSetup = false;
  let pendingStatusReads = 0;
  const pendingMcpStatus = Promise.withResolvers<void>();
  let eventPolls = 0;
  let eventFailures = 0;
  let openedTabs = 0;
  const openedProfiles: Array<string | undefined> = [];
  const failedPoll = Promise.withResolvers<void>();
  let toolReply = Promise.withResolvers<void>();
  const toolResponses: unknown[] = [];
  let creationModel = "claude-haiku-5-5";
  let runtimeModel = creationModel;
  const sentModels: string[] = [];
  let runtimeEffort: EffortLevel = input?.effort ?? "medium";
  const sentEfforts: EffortLevel[] = [];
  let scriptedEvents: unknown[] = [];
  const fetch = async (url: string, init?: RequestInit) => {
    const method = init?.method ?? "GET";
    const body = typeof init?.body === "string" ? decodeJson(init.body) : undefined;
    calls.push({ path: url, method, body });
    if (url.endsWith("/usage"))
      return input?.requirePersistentProfile && openedProfiles.at(-1) !== "default"
        ? Response.json({}, { status: 403 })
        : Response.json(usage);
    if (url.endsWith("/environments"))
      return Response.json({
        environments: [
          { kind: "anthropic_cloud", state: "active", name: "Default", environment_id: "env_test" },
        ],
      });
    if (url.endsWith("/events")) {
      if (method === "GET") {
        eventPolls++;
        if (eventFailures > 0) {
          eventFailures--;
          failedPoll.resolve();
          throw new Error("Injected browser disconnect");
        }
        return Response.json({ data: [...controlReplies, ...scriptedEvents] });
      }
      {
        const payload = (
          body as {
            events: Array<{
              payload: {
                type: string;
                request_id: string;
                request: {
                  subtype: string;
                  model?: string;
                  settings?: { effortLevel?: EffortLevel | null };
                };
              };
            }>;
          }
        ).events[0]?.payload;
        const setupFailed =
          payload?.type === "control_request" &&
          payload.request.subtype === "mcp_set_servers" &&
          setupFailures-- > 0;
        if (
          payload?.type === "control_request" &&
          ["mcp_set_servers", "mcp_reconnect"].includes(payload.request.subtype) &&
          !setupFailed &&
          !(payload.request.subtype === "mcp_reconnect" && reconnectFails)
        ) {
          pendingStatusReads = input?.setupStatusLag ?? 0;
          mcpStatus = holdMcpSetup || pendingStatusReads > 0 ? "pending" : "connected";
          for (const event of holdMcpSetup ? [] : (input?.initializationEvents ?? []))
            controlReplies.push({
              event_id: `initialization:${++sequence}`,
              sequence_num: String(sequence),
              payload: event,
            });
        }
        if (
          payload?.type === "control_request" &&
          payload.request.subtype === "set_model" &&
          !input?.modelSwitchFails
        )
          runtimeModel = payload.request.model ?? runtimeModel;
        if (
          payload?.type === "control_request" &&
          payload.request.subtype === "apply_flag_settings" &&
          payload.request.settings?.effortLevel !== undefined
        )
          runtimeEffort = payload.request.settings.effortLevel ?? "medium";
        if (payload?.request?.subtype === "mcp_status" && mcpStatus === "pending") {
          pendingMcpStatus.resolve();
        }
        if (payload?.type === "control_request")
          controlReplies.push({
            event_id: `reply:${++sequence}`,
            sequence_num: String(sequence),
            payload: {
              type: "control_response",
              response: {
                subtype:
                  (payload.request.subtype === "set_model" && input?.modelSwitchFails) ||
                  (payload.request.subtype === "apply_flag_settings" && input?.reasoningFails) ||
                  (payload.request.subtype === "mcp_reconnect" && reconnectFails)
                    ? "error"
                    : "success",
                request_id: payload.request_id,
                response: setupFailed
                  ? { errors: { "t3-code": "injected failure" } }
                  : payload.request.subtype === "get_settings"
                    ? { applied: { effort: input?.appliedEffortOverride ?? runtimeEffort } }
                    : payload.request.subtype === "mcp_status"
                      ? {
                          mcpServers: [
                            {
                              name: "existing-cloud-tools",
                              source: "dynamic",
                              config: { type: "http", url: "https://example.test/mcp" },
                            },
                            ...(mcpStatus === "missing"
                              ? []
                              : [
                                  {
                                    name: "t3-code",
                                    status: mcpStatus,
                                    source: "sdk",
                                    config: { type: "sdk", name: "t3-code" },
                                  },
                                ]),
                          ],
                        }
                      : {},
              },
            },
          });
        if (payload?.request?.subtype === "mcp_status" && pendingStatusReads > 0) {
          pendingStatusReads--;
          if (pendingStatusReads === 0) mcpStatus = "connected";
        }
        if (payload?.type === "control_response") {
          toolResponses.push(payload);
          toolReply.resolve();
        }
      }
      if (
        (body as { events?: { payload: { type: string } }[] })?.events?.some(
          (e) => e.payload.type === "user",
        )
      ) {
        if (input?.promptRejectStatus !== undefined)
          return Response.json({}, { status: input.promptRejectStatus });
        sentModels.push(runtimeModel);
        sentEfforts.push(runtimeEffort);
        if (!sent)
          scriptedEvents = (input?.events ?? [])
            .map((event) => event as { event_id: string; sequence_num: string; payload: unknown })
            .sort((a, b) => Number(BigInt(a.sequence_num) - BigInt(b.sequence_num)))
            .map((event) => ({ ...event, sequence_num: String(++sequence) }));
        sent = true;
        if (input?.promptResponseLost) throw new Error("Injected lost prompt response");
      }
      return Response.json({});
    }
    if (url === "/v1/code/sessions" && input?.createFails)
      return Response.json({}, { status: 503 });
    if (url === "/v1/code/sessions") {
      creationModel = (body as { config: { model: string } }).config.model;
      runtimeModel = creationModel;
      return Response.json({
        deduplicated: false,
        session: { id: "cse_test", config: { model: creationModel } },
      });
    }
    return Response.json({
      response_shape: { id: "cse_test", config: { model: creationModel } },
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
      if (request.operation === "open") {
        openedTabs++;
        const opened = request.input as { profileId?: string; tabId?: string };
        openedProfiles.push(opened.profileId);
        if (input?.rejectReusedTab && opened.tabId !== undefined)
          return Effect.fail(
            new PreviewAutomationNoAvailableHostError({
              operation: request.operation,
              environmentId,
              threadId,
              providerSessionId: "test",
              providerInstanceId: ProviderInstanceId.make("cloud-test"),
              cause: "Owned tab no longer exists",
            }),
          );
        return Effect.succeed({ tabId: PreviewTabId.make("cloud-test-tab") } as A);
      }
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
        rpc.method === "tools/call" &&
        (rpc.params as { name?: string })?.name === "t3_workspace_exec" &&
        input?.workspaceExec !== undefined
      ) {
        const result = yield* input.workspaceExec(
          (rpc.params as { arguments?: unknown }).arguments,
        );
        return HttpClientResponse.fromWeb(
          request,
          Response.json({ jsonrpc: "2.0", id: rpc.id, result }),
        );
      }
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
    const open = (options?: {
      resume?: boolean;
      durableCloudSequence?: string;
      model?: string;
      effort?: EffortLevel;
      recoverPendingCloudTurn?: boolean;
    }) => {
      const effort = options?.effort ?? input?.effort;
      return runner
        .open({
          threadId,
          providerSessionId: ProviderSessionId.make("cloud-test-session"),
          ...(options?.recoverPendingCloudTurn === undefined
            ? {}
            : { recoverPendingCloudTurn: options.recoverPendingCloudTurn }),
          ...(options?.durableCloudSequence === undefined
            ? {}
            : { durableCloudSequence: options.durableCloudSequence }),
          options: {
            ...(options?.resume ? { resume: sessionId } : { sessionId }),
            model: options?.model ?? input?.model ?? "claude-haiku-5-5",
            ...(effort === undefined ? {} : { effort }),
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
    };
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
      sentModels,
      sentEfforts,
      waitForToolReply: Effect.promise(() => toolReply.promise).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            toolReply = Promise.withResolvers<void>();
          }),
        ),
      ),
      eventPolls: () => eventPolls,
      openedTabs: () => openedTabs,
      openedProfiles,
      recycleCloudWorker: (hold = false) => {
        mcpStatus = "missing";
        holdMcpSetup = hold;
      },
      waitPendingMcp: Effect.promise(() => pendingMcpStatus.promise),
      completeMcpSetup: (connected: boolean) => {
        mcpStatus = connected ? "connected" : "failed";
        controlReplies.push({
          event_id: `new-handshake:${++sequence}`,
          sequence_num: String(sequence),
          payload: {
            type: "control_request",
            request_id: "new-handshake",
            request: {
              subtype: "mcp_message",
              server_name: "t3-code",
              message: { jsonrpc: "2.0", id: 302, method: "tools/list" },
            },
          },
        });
      },
      disconnectMcp: (fail = false) => {
        mcpStatus = "failed";
        reconnectFails = fail;
      },
      failMcpSetup: () => {
        setupFailures = 1;
      },
      failEventPolls: (count: number) => {
        eventFailures = count;
      },
      waitFailedPoll: Effect.promise(() => failedPoll.promise),
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
      forgetToolOutcome: (requestId: string, legacy = false) =>
        fs.readFileString(bindingPath).pipe(
          Effect.map(
            Schema.decodeUnknownSync(
              Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
            ),
          ),
          Effect.flatMap((saved) =>
            fs.writeFileString(
              bindingPath,
              json({
                ...saved,
                toolRequests: {
                  [requestId]: {
                    state: "started",
                    ...(legacy
                      ? {}
                      : {
                          commandRequestId: (
                            saved.toolRequests as Record<string, { commandRequestId?: string }>
                          )[requestId]?.commandRequestId,
                        }),
                  },
                },
              }),
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
    | import("effect/process/ChildProcessSpawner").ChildProcessSpawner
  >,
) => effect.pipe(Effect.scoped, Effect.provide(NodeServices.layer));

describe("Claude cloud guarded transport", () => {
  it.live("waits for cloud status to catch up after the tools/list reply is delivered", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({
          workspace: true,
          setupStatusLag: 3,
          initializationEvents: [
            {
              type: "control_request",
              request_id: "lagged-handshake",
              request: {
                subtype: "mcp_message",
                server_name: "t3-code",
                message: { jsonrpc: "2.0", id: 303, method: "tools/list" },
              },
            },
          ],
        });
        const opened = yield* f.open().pipe(Effect.forkChild);
        yield* f.waitForToolReply;
        const session = yield* Fiber.join(opened);
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Only send after cloud confirms connected" },
        });
        expect(f.sentModels).toHaveLength(1);
        yield* session.close;
      }),
    ),
  );
  it.effect("closes a failed reconnect's query so the same conversation can be retried", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true });
        const session = yield* f.open();
        f.disconnectMcp(true);
        const offered = yield* session
          .offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Do not send while pending" },
          })
          .pipe(Effect.result);
        expect(offered._tag).toBe("Failure");
        expect(f.sentModels).toHaveLength(0);
        expect(yield* session.messages.pipe(Stream.runCollect)).toEqual([]);
        f.disconnectMcp();
        const retried = yield* f.open({ resume: true });
        yield* retried.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Resume after reconnecting" },
        });
        expect(f.sentModels).toHaveLength(1);
        yield* retried.close;
      }),
    ),
  );
  it.effect.each([false, true])(
    "reconnects workspace tools before a new turn after idle worker recycling (setup failure: %s)",
    (failSetup) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "First task" },
          });
          f.pushEvent({ type: "result", subtype: "success", result: "Done" });
          const result = yield* session.messages.pipe(
            Stream.filter((m) => m.type === "result"),
            Stream.runHead,
          );
          if (result._tag !== "Some") throw new Error("Missing first result");
          const checkpoint = session.cloudEventSequence?.(result.value);
          if (checkpoint === undefined) throw new Error("Missing checkpoint");
          f.recycleCloudWorker();
          if (failSetup) f.failMcpSetup();
          const offered = yield* session
            .offer(
              {
                type: "user",
                session_id: "",
                parent_tool_use_id: null,
                message: { role: "user", content: "PR check failed; inspect the logs" },
              },
              checkpoint,
            )
            .pipe(Effect.result);
          expect(offered._tag).toBe(failSetup ? "Failure" : "Success");
          expect(f.sentModels).toHaveLength(failSetup ? 1 : 2);
          const posts = f.calls.flatMap(
            (c) =>
              (
                c.body as {
                  events?: Array<{ payload: { type: string; request?: { subtype: string } } }>;
                }
              )?.events ?? [],
          );
          expect(
            posts.filter((e) => e.payload.request?.subtype === "mcp_set_servers"),
          ).toHaveLength(2);
          expect(
            JSON.stringify(posts.filter((e) => e.payload.request?.subtype === "mcp_set_servers")),
          ).toContain('"existing-cloud-tools":{"type":"http","url":"https://example.test/mcp"}');
          if (!failSetup) {
            expect(
              posts.findLastIndex((e) => e.payload.request?.subtype === "mcp_set_servers"),
            ).toBeLessThan(posts.findLastIndex((e) => e.payload.type === "user"));
          }
          yield* session.close;
        }),
      ),
  );
  it.effect.each([false, true])(
    "waits for a fresh handshake after recycling instead of reusing old readiness (connected: %s)",
    (connected) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({
            workspace: true,
            initializationEvents: [
              {
                type: "control_request",
                request_id: "initial-handshake",
                request: {
                  subtype: "mcp_message",
                  server_name: "t3-code",
                  message: { jsonrpc: "2.0", id: 301, method: "tools/list" },
                },
              },
            ],
          });
          const session = yield* f.open();
          yield* f.waitForToolReply;
          f.recycleCloudWorker(true);
          const offered = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Wait for the new workspace connection" },
            })
            .pipe(Effect.result, Effect.forkChild);
          yield* f.waitPendingMcp;
          yield* TestClock.adjust("2 seconds");
          expect(offered.pollUnsafe()).toBeUndefined();
          expect(f.sentModels).toHaveLength(0);
          f.completeMcpSetup(connected);
          const result = yield* Fiber.join(offered);
          expect(result._tag).toBe(connected ? "Success" : "Failure");
          expect(f.sentModels).toHaveLength(connected ? 1 : 0);
          yield* session.close;
        }),
      ),
  );
  it.effect.each([false, true])(
    "reconnects a failed SDK connection before admitting a prompt (reconnect failure: %s)",
    (failReconnect) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          f.disconnectMcp(failReconnect);
          const offered = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Inspect CI" },
            })
            .pipe(Effect.result);
          expect(offered._tag).toBe(failReconnect ? "Failure" : "Success");
          expect(f.sentModels).toHaveLength(failReconnect ? 0 : 1);
          expect(JSON.stringify(f.calls)).toContain('"subtype":"mcp_reconnect"');
          yield* session.close;
        }),
      ),
  );
  it.effect(
    "recovers before clients report profiles using the persistent signed-in browser profile",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ requirePersistentProfile: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Keep this cloud conversation" },
          });
          f.pushEvent({
            type: "assistant",
            message: { content: [{ type: "text", text: "Saved reply" }] },
          });
          yield* TestClock.adjust("200 millis");
          const saved = yield* session.messages.pipe(Stream.runHead);
          if (saved._tag !== "Some") throw new Error("Missing saved snapshot");
          const checkpoint = session.cloudEventSequence?.(saved.value);
          if (checkpoint === undefined) throw new Error("Missing snapshot checkpoint");
          yield* session.close;
          f.pushEvent({
            type: "result",
            subtype: "success",
            result: "Recovered in the same conversation",
          });
          const recovered = yield* f.open({
            resume: true,
            recoverPendingCloudTurn: true,
            durableCloudSequence: checkpoint,
          });
          if (recovered.resumePendingCloudTurn === undefined)
            throw new Error("Missing recovery admission");
          yield* recovered.resumePendingCloudTurn;
          const frames = yield* recovered.messages.pipe(Stream.take(2), Stream.runCollect);
          expect(frames.map((message) => message.type)).toEqual(["assistant", "result"]);
          expect(f.openedProfiles).toEqual(["default", "default"]);
          expect(f.sentModels).toHaveLength(1);
          expect(f.calls.filter((call) => call.path === "/v1/code/sessions")).toHaveLength(1);
          yield* recovered.close;
        }),
      ),
  );
  it.effect(
    "keeps the persistent profile when a disappeared browser tab needs a fresh reconnect",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ requirePersistentProfile: true, rejectReusedTab: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Reconnect this tab" },
          });
          f.failEventPolls(3);
          f.pushEvent({ type: "result", subtype: "success", result: "Reattached" });
          yield* TestClock.adjust("200 millis");
          yield* f.waitFailedPoll;
          yield* TestClock.adjust("10 seconds");
          const result = yield* session.messages.pipe(Stream.runHead);
          expect(result._tag === "Some" && result.value.type).toBe("result");
          expect(f.openedProfiles).toEqual(["default", "default", "default"]);
          expect(f.sentModels).toHaveLength(1);
          yield* session.close;
        }),
      ),
  );
  it.effect.each([403, 503])(
    "surfaces a definite coding POST HTTP %s failure without resending",
    (status) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ promptRejectStatus: status });
          const session = yield* f.open();
          const offered = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Rejected once" },
            })
            .pipe(Effect.result);
          expect(offered._tag).toBe("Failure");
          expect(JSON.stringify(offered)).toContain("ClaudeCloudHttpError");
          expect(f.sentModels).toHaveLength(0);
          yield* session.close;
        }),
      ),
  );
  it.effect(
    "continues a remotely accepted prompt whose POST response was lost without resending it",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({
            promptResponseLost: true,
            events: [
              {
                event_id: "lost-post-result",
                sequence_num: "1",
                payload: { type: "result", subtype: "success", result: "Accepted once" },
              },
            ],
          });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Accept once" },
          });
          const result = yield* session.messages.pipe(Stream.runHead);
          expect(result._tag === "Some" && result.value.type).toBe("result");
          expect(f.sentModels).toHaveLength(1);
          expect(f.calls.filter((c) => c.path === "/v1/code/sessions")).toHaveLength(1);
          yield* session.close;
        }),
      ),
  );
  it.effect("reconnects a disconnected browser without another coding POST or conversation", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture();
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Keep this turn alive" },
        });
        f.failEventPolls(6);
        f.pushEvent({ type: "result", subtype: "success", result: "Reconnected" });
        yield* TestClock.adjust("200 millis");
        yield* f.waitFailedPoll;
        yield* TestClock.adjust("10 seconds");
        const result = yield* session.messages.pipe(Stream.runHead);
        expect(result._tag === "Some" && result.value.type).toBe("result");
        expect(f.openedTabs()).toBe(2);
        expect(f.sentModels).toEqual(["claude-haiku-5-5"]);
        expect(f.calls.filter((c) => c.path === "/v1/code/sessions")).toHaveLength(1);
        yield* session.close;
      }),
    ),
  );
  it.effect("drains an interrupted turn only after explicit continuation admission", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true });
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          uuid: "00000000-0000-4000-8000-000000000123",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Original task" },
        });
        f.pushEvent({
          type: "assistant",
          uuid: "saved-reply",
          message: { content: [{ type: "text", text: "Already saved" }] },
        });
        yield* TestClock.adjust("200 millis");
        const first = yield* session.messages.pipe(Stream.runHead);
        if (first._tag !== "Some") throw new Error("Missing initial reply");
        const checkpoint = session.cloudEventSequence?.(first.value);
        if (checkpoint === undefined) throw new Error("Missing initial checkpoint");
        yield* session.close;
        f.pushEvent({
          type: "control_request",
          request_id: "late-write",
          request: {
            subtype: "mcp_message",
            server_name: "t3-code",
            message: {
              jsonrpc: "2.0",
              id: 205,
              method: "tools/call",
              params: { name: "t3_workspace_write", arguments: { path: "note.txt", text: "Once" } },
            },
          },
        });
        f.pushEvent({ type: "assistant", uuid: "new-reply", message: { content: [] } });
        f.pushEvent({ type: "result", subtype: "success", result: "Recovered" });
        const resumed = yield* f.open({
          resume: true,
          recoverPendingCloudTurn: true,
          durableCloudSequence: checkpoint,
        });
        expect(resumed.pendingCloudTurn?.promptUuid).toBe("00000000-0000-4000-8000-000000000123");
        expect(
          f.rpcCalls.filter((c) => JSON.stringify(c).includes("t3_workspace_write")),
        ).toHaveLength(0);
        const refused = yield* resumed
          .offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "A genuinely new task must wait" },
          })
          .pipe(Effect.result);
        expect(refused._tag).toBe("Failure");
        if (resumed.resumePendingCloudTurn === undefined)
          throw new Error("Missing recovery admission");
        yield* resumed.resumePendingCloudTurn;
        const recovered = yield* resumed.messages.pipe(Stream.take(3), Stream.runCollect);
        expect(recovered.map((m) => m.type)).toEqual(["assistant", "assistant", "result"]);
        expect(resumed.cloudEventWasDurable?.(recovered[0]!)).toBe(true);
        expect(resumed.cloudEventWasDurable?.(recovered[1]!)).toBe(false);
        yield* f.waitForToolReply;
        expect(
          f.rpcCalls.filter((c) => JSON.stringify(c).includes("t3_workspace_write")),
        ).toHaveLength(1);
        expect(f.sentModels).toHaveLength(1);
        const resultCursor = resumed.cloudEventSequence?.(recovered[2]!);
        if (resultCursor === undefined) throw new Error("Missing recovery result checkpoint");
        yield* resumed.offer(
          {
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Now admit the new task" },
          },
          resultCursor,
        );
        expect(f.sentModels).toHaveLength(2);
        expect(resumed.pendingCloudTurn).toBeUndefined();
        expect(f.calls.filter((c) => c.path === "/v1/code/sessions")).toHaveLength(1);
        yield* resumed.close;
      }),
    ),
  );
  it.effect.each(["t3_workspace_write", "t3_workspace_exec"] as const)(
    "recovers an uncertain %s using its effect ledger without blindly repeating it",
    (name) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Run this once" },
          });
          f.pushEvent({
            type: "control_request",
            request_id: "recoverable-command",
            request: {
              subtype: "mcp_message",
              server_name: "t3-code",
              message: {
                jsonrpc: "2.0",
                id: 206,
                method: "tools/call",
                params: {
                  name,
                  arguments: { command: "make verify", requestId: "untrusted-agent-key" },
                },
              },
            },
          });
          yield* TestClock.adjust("200 millis");
          yield* f.waitForToolReply;
          yield* session.close;
          yield* f.forgetToolOutcome("recoverable-command");
          const resumed = yield* f.open({ resume: true, recoverPendingCloudTurn: true });
          if (resumed.resumePendingCloudTurn === undefined)
            throw new Error("Missing recovery admission");
          yield* resumed.resumePendingCloudTurn;
          yield* f.waitForToolReply;
          const calls = f.rpcCalls.filter((call) => JSON.stringify(call).includes(name));
          if (name === "t3_workspace_exec") {
            expect(calls).toHaveLength(2);
            const first = calls[0];
            const next = calls[1];
            if (first === undefined || next === undefined) throw new Error("Missing command calls");
            const firstArgs = (first.params as { arguments: { requestId: string } }).arguments;
            const nextArgs = (next.params as { arguments: { requestId: string } }).arguments;
            expect(firstArgs.requestId).toMatch(/^cloud:[a-f0-9]{64}$/);
            expect(nextArgs.requestId).toBe(firstArgs.requestId);
          } else {
            expect(calls).toHaveLength(1);
            expect(JSON.stringify(f.toolResponses.at(-1))).toContain("may have executed");
          }
          expect(f.sentModels).toHaveLength(1);
          yield* resumed.close;
        }),
      ),
  );
  it.live(
    "deduplicates explicit command keys across new cloud control requests without merging omitted keys",
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped();
          const jobs = yield* makeCommandJobs.pipe(
            Effect.provide(ServerConfig.ServerConfig.layerTest(directory, `${directory}/state`)),
          );
          const owner = {
            environmentId,
            threadId,
            projectId: "project-football",
            cwd: directory,
            workspaceIdentity: "fixture",
          };
          const jobIds: string[] = [];
          let rejected = 0;
          const f = yield* fixture({
            workspace: true,
            workspaceExec: (arguments_) =>
              decodeWorkspaceExec(arguments_).pipe(
                Effect.flatMap((input) => jobs.start(owner, { ...input, yieldMs: 10000 })),
                Effect.match({
                  onSuccess: (job) => {
                    jobIds.push(job.jobId);
                    return { isError: false, content: [{ type: "text", text: json(job) }] };
                  },
                  onFailure: () => {
                    rejected++;
                    return {
                      isError: true,
                      content: [
                        {
                          type: "text",
                          text: "The command key already belongs to different work.",
                        },
                      ],
                    };
                  },
                }),
              ),
          });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Run commands once" },
          });
          const key = "client-key-" + "x".repeat(190);
          const command = "printf x >> starts";
          const request = (controlId: string, arguments_: Record<string, unknown>) => ({
            type: "control_request",
            request_id: controlId,
            request: {
              subtype: "mcp_message",
              server_name: "t3-code",
              message: {
                jsonrpc: "2.0",
                id: controlId,
                method: "tools/call",
                params: { name: "t3_workspace_exec", arguments: arguments_ },
              },
            },
          });
          f.pushEvent(request("first", { command, requestId: key }));
          yield* f.waitForToolReply;
          f.pushEvent(request("second", { command, requestId: key }));
          yield* f.waitForToolReply;
          f.pushEvent(request("first", { command, requestId: key }));
          f.pushEvent(
            request("changed-command", { command: "printf unexpected >> starts", requestId: key }),
          );
          yield* f.waitForToolReply;
          f.pushEvent(request(key, { command }));
          yield* f.waitForToolReply;
          f.pushEvent(request("another-control", { command }));
          yield* f.waitForToolReply;
          const relayed = f.rpcCalls.filter(
            (rpc) => (rpc.params as { name?: string })?.name === "t3_workspace_exec",
          );
          expect(relayed).toHaveLength(5);
          const keys = relayed.map(
            (rpc) => (rpc.params as { arguments: { requestId: string } }).arguments.requestId,
          );
          expect(keys.every((value) => /^cloud:[a-f0-9]{64}$/.test(value))).toBe(true);
          expect(keys[1]).toBe(keys[0]);
          expect(keys[2]).toBe(keys[0]);
          expect(keys[3]).not.toBe(keys[0]);
          expect(keys[4]).not.toBe(keys[3]);
          expect(jobIds[1]).toBe(jobIds[0]);
          expect(new Set(jobIds).size).toBe(3);
          expect(rejected).toBe(1);
          expect(yield* fs.readFileString(`${directory}/starts`)).toBe("xxx");
          yield* session.close;
        }),
      ),
    15000,
  );
  it.effect("preserves a legacy started command's original job key during recovery", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true });
        const session = yield* f.open();
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Recover old work" },
        });
        yield* session.close;
        yield* f.forgetToolOutcome("legacy-command", true);
        f.pushEvent({
          type: "control_request",
          request_id: "legacy-command",
          request: {
            subtype: "mcp_message",
            server_name: "t3-code",
            message: {
              jsonrpc: "2.0",
              id: 301,
              method: "tools/call",
              params: {
                name: "t3_workspace_exec",
                arguments: { command: "make verify", requestId: "old-client-key" },
              },
            },
          },
        });
        const resumed = yield* f.open({ resume: true, recoverPendingCloudTurn: true });
        if (resumed.resumePendingCloudTurn === undefined) throw new Error("Missing recovery");
        yield* resumed.resumePendingCloudTurn;
        yield* f.waitForToolReply;
        const relayed = f.rpcCalls.find(
          (rpc) => (rpc.params as { name?: string })?.name === "t3_workspace_exec",
        );
        if (relayed === undefined) throw new Error("Missing legacy command recovery");
        expect((relayed.params as { arguments: { requestId: string } }).arguments.requestId).toBe(
          "cloud:session_test:legacy-command",
        );
        yield* resumed.close;
      }),
    ),
  );
  it.effect.each(["low", "medium", "high", "xhigh", "max"] as const)(
    "applies and confirms %s reasoning before sending the prompt",
    (effort) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ workspace: true, effort });
          const session = yield* f.open();
          expect(f.calls.find((c) => c.path === "/v1/code/sessions")?.body).toMatchObject({
            config: { effort_level: effort },
          });
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Use the selected effort" },
          });
          expect(f.sentEfforts).toEqual([effort]);
          yield* session.close;
        }),
      ),
  );
  it.effect.each(["mismatch", "rejected"] as const)(
    "blocks the coding prompt if reasoning is %s",
    (kind) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({
            workspace: true,
            effort: "high",
            ...(kind === "mismatch"
              ? { appliedEffortOverride: "low" as const }
              : { reasoningFails: true }),
          });
          const session = yield* f.open();
          const offered = yield* session
            .offer({
              type: "user",
              session_id: "",
              parent_tool_use_id: null,
              message: { role: "user", content: "Must not use the wrong effort" },
            })
            .pipe(Effect.result);
          expect(offered._tag).toBe("Failure");
          expect(JSON.stringify(offered)).toContain("reasoning_unavailable");
          expect(f.sentEfforts).toEqual([]);
          yield* session.close;
        }),
      ),
  );
  it.effect("updates reasoning live and reapplies the chosen effort on resume", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true, effort: "low" });
        const session = yield* f.open();
        yield* session.applyFlagSettings({ effortLevel: "xhigh" });
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "First task" },
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
        yield* session.close;
        const resumed = yield* f.open({
          resume: true,
          durableCloudSequence: checkpoint,
          effort: "max",
        });
        yield* resumed.offer(
          {
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Second task" },
          },
          checkpoint,
        );
        expect(f.sentEfforts).toEqual(["xhigh", "max"]);
        yield* resumed.close;
      }),
    ),
  );
  it.effect.each(["claude-haiku-5-5", "claude-opus-5-5", "claude-fable-5-1"])(
    "creates and sends with the explicitly selected model %s",
    (model) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ model, workspace: true });
          const session = yield* f.open();
          yield* session.offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Selected model task" },
          });
          expect(f.calls.find((c) => c.path === "/v1/code/sessions")?.body).toMatchObject({
            config: { model },
            events: [],
          });
          expect(f.sentModels).toEqual([model]);
          yield* session.close;
        }),
      ),
  );
  it.effect("switches an existing conversation from Opus to Fable before its next prompt", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true });
        const session = yield* f.open();
        yield* session.setModel("opus");
        yield* session.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "First task" },
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
        yield* session.setModel("fable");
        yield* session.offer(
          {
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Second task" },
          },
          checkpoint,
        );
        expect(f.sentModels).toEqual(["claude-opus-5-5", "claude-fable-5-1"]);
        expect(
          f.calls.filter((c) => c.path === "/v1/code/sessions" && c.method === "POST"),
        ).toHaveLength(1);
        yield* session.close;
      }),
    ),
  );
  it.effect("applies a different selected model when resuming a completed cloud conversation", () =>
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
        const resumed = yield* f.open({
          resume: true,
          durableCloudSequence: checkpoint,
          model: "claude-opus-5-5",
        });
        yield* resumed.offer(
          {
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Resumed task" },
          },
          checkpoint,
        );
        expect(f.sentModels).toEqual(["claude-haiku-5-5", "claude-opus-5-5"]);
        expect(
          f.calls.filter((c) => c.path === "/v1/code/sessions" && c.method === "POST"),
        ).toHaveLength(1);
        yield* resumed.close;
      }),
    ),
  );
  it.effect("does not send a coding prompt when the remote model change is rejected", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true, modelSwitchFails: true });
        const session = yield* f.open();
        const offered = yield* session
          .offer({
            type: "user",
            session_id: "",
            parent_tool_use_id: null,
            message: { role: "user", content: "Must not silently use another model" },
          })
          .pipe(Effect.result);
        expect(offered._tag).toBe("Failure");
        expect(f.sentModels).toEqual([]);
        yield* session.close;
      }),
    ),
  );
  it.effect("blocks model changes while work is active or paid usage is enabled", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ workspace: true });
        const session = yield* f.open();
        f.setPaid();
        expect((yield* session.setModel("opus").pipe(Effect.result))._tag).toBe("Failure");
        expect(JSON.stringify(f.calls)).not.toContain('"model":"claude-opus-5-5"');
        yield* session.close;
        const active = yield* fixture({ workspace: true });
        const activeSession = yield* active.open();
        yield* activeSession.offer({
          type: "user",
          session_id: "",
          parent_tool_use_id: null,
          message: { role: "user", content: "Active task" },
        });
        expect((yield* activeSession.setModel("fable").pipe(Effect.result))._tag).toBe("Failure");
        expect(JSON.stringify(active.calls)).not.toContain('"model":"claude-fable-5-1"');
        yield* activeSession.close;
      }),
    ),
  );
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
          const first = yield* session.messages.pipe(Stream.runHead);
          if (first._tag !== "Some") throw new Error("Missing first reply");
          const checkpoint = session.cloudEventSequence?.(first.value);
          if (checkpoint === undefined) throw new Error("Missing checkpoint");
          yield* session.close;
          const resumed = yield* f
            .open({ resume: true, durableCloudSequence: checkpoint })
            .pipe(Effect.result);
          expect(resumed._tag).toBe("Failure");
          expect(JSON.stringify(resumed)).toContain("delivery_pending");
          expect(yield* f.readBinding).toMatchObject({
            inbox: expect.arrayContaining([
              expect.objectContaining({ event_id: "unread", payload: expect.anything() }),
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
  it.effect.each(["claude-haiku-5-5", "claude-opus-5-5", "claude-fable-5-1"])(
    "never creates a %s session when paid usage is enabled",
    (model) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture({ paid: true, model });
          const result = yield* f.open().pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          expect(f.calls.some((c) => c.method === "POST")).toBe(false);
        }),
      ),
  );
  it.effect("rejects unsupported models before even opening the browser", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture({ model: "claude-opus-5-5[fast]" });
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
          events: [{ payload: { request: { subtype: "set_model", model: "claude-haiku-5-5" } } }],
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
