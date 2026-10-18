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
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
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
}) {
  let usage: unknown = input?.paid ? { ...funded(), extra_usage: { is_enabled: true } } : funded();
  const calls: Array<{ path: string; method: string; body?: unknown }> = [];
  const sent = Promise.withResolvers<void>();
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
        if (input?.events) await sent.promise;
        return Response.json({ data: input?.events ?? [] });
      }
      if (
        (body as { events?: { payload: { type: string } }[] })?.events?.some(
          (e) => e.payload.type === "user",
        )
      )
        sent.resolve();
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
  const broker = PreviewAutomationBroker.PreviewAutomationBroker.of({
    connect: () => Effect.succeed(Stream.empty),
    focusHost: () => Effect.void,
    respond: () => Effect.void,
    invoke: <A>(request: PreviewAutomationBroker.PreviewAutomationInvokeInput) => {
      if (request.operation === "open")
        return Effect.succeed({ tabId: PreviewTabId.make("cloud-test-tab") } as A);
      if (request.operation === "navigate") return Effect.succeed({} as A);
      const expression = (request.input as { expression: string }).expression;
      return Effect.tryPromise({
        try: async () =>
          (await NodeVM.runInNewContext(expression, {
            fetch,
            location: { origin: "https://claude.ai", pathname: "/code" },
            AbortSignal,
          })) as A,
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
    const open = () =>
      runner.open({
        threadId,
        providerSessionId: ProviderSessionId.make("cloud-test-session"),
        options: {
          sessionId,
          model: input?.model ?? "claude-haiku-5-5",
          tools: [],
          permissionMode: "bypassPermissions",
        },
      });
    return {
      runner,
      open,
      calls,
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
        const resumed = yield* f.open();
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
});
