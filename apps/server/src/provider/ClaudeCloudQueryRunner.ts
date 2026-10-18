import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { ProviderInstanceId, type ClaudeSettings, type PreviewTabId } from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PreviewAutomationBroker from "../mcp/PreviewAutomationBroker.ts";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as ProcessRunner from "../processRunner.ts";
import {
  ClaudeAgentSdkQueryRunnerError,
  type ClaudeAgentSdkQueryRunnerShape,
  type ClaudeAgentSdkQueryOpenInput,
  type ClaudeAgentSdkQuerySession,
} from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { resolveClaudeHomePath } from "./Drivers/ClaudeHome.ts";
import {
  CLAUDE_CLOUD_MODEL,
  ClaudeCloudGuardError,
  orderedClaudeCloudEvents,
  verifyClaudeCloudCredit,
  verifyClaudeCloudEnvironment,
  verifyClaudeCloudModel,
} from "./ClaudeCloudPolicy.ts";

const Binding = Schema.Struct({
  organizationId: Schema.String,
  cloudId: Schema.String,
  sequence: Schema.String,
});
const Account = Schema.Struct({ oauthAccount: Schema.Struct({ organizationUuid: Schema.String }) });
const SessionInfo = Schema.Struct({
  response_shape: Schema.Struct({
    id: Schema.String,
    config: Schema.Struct({ model: Schema.String }),
  }),
});
const EnvironmentList = Schema.Struct({
  environments: Schema.Array(
    Schema.Struct({
      kind: Schema.String,
      environment_id: Schema.String,
      name: Schema.String,
      state: Schema.String,
    }),
  ),
});
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const relayedTypes = new Set([
  "assistant",
  "user",
  "result",
  "system",
  "stream_event",
  "rate_limit_event",
  "tool_progress",
  "tool_use_summary",
  "auth_status",
]);

export const makeClaudeCloudQueryRunner = Effect.fn("makeClaudeCloudQueryRunner")(function* (
  config: ClaudeSettings,
  environment: NodeJS.ProcessEnv,
  instanceId: ProviderInstanceId,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  // CLI-only runtimes can still construct ordinary local Claude instances without a browser.
  const maybeBroker = yield* Effect.serviceOption(PreviewAutomationBroker.PreviewAutomationBroker);
  const maybeIdentity = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironmentIdentity);
  const maybeProcesses = yield* Effect.serviceOption(ProcessRunner.ProcessRunner);
  if (Option.isNone(maybeBroker) || Option.isNone(maybeIdentity) || Option.isNone(maybeProcesses)) {
    return yield* new ClaudeCloudGuardError({ reason: "account_mismatch" });
  }
  const broker = maybeBroker.value;
  const identity = maybeIdentity.value;
  const processes = maybeProcesses.value;
  const server = yield* ServerConfig.ServerConfig;
  const ownerScope = yield* Effect.scope;
  const configDir = yield* resolveClaudeHomePath(config, environment);
  const accountPath =
    config.homePath.trim() || environment.CLAUDE_CONFIG_DIR?.trim()
      ? path.join(configDir, ".claude.json")
      : path.join(configDir, "..", ".claude.json");
  const bindingDir = path.join(server.stateDir, "claude-cloud-sessions");
  yield* fs.makeDirectory(bindingDir, { recursive: true });

  const wrap = (method: string) => (cause: unknown) =>
    new ClaudeAgentSdkQueryRunnerError({ method, cause });
  const unsupported = (method: string) =>
    Effect.fail(
      new ClaudeAgentSdkQueryRunnerError({
        method,
        cause: new ClaudeCloudGuardError({ reason: "unsupported_policy" }),
      }),
    );

  return {
    allocateSessionId: crypto.randomUUIDv4().pipe(Effect.mapError(wrap("allocateSessionId"))),
    assertComplete: Effect.void,
    forkSession: () => unsupported("forkSession"),
    subagentLaunchToolUseId: () => Effect.succeed(null),
    open: (input: ClaudeAgentSdkQueryOpenInput) =>
      Effect.gen(function* () {
        yield* Effect.try(() => {
          verifyClaudeCloudEnvironment(environment);
          verifyClaudeCloudModel(input.options.model);
          if (input.options.permissionMode !== "bypassPermissions") {
            throw new ClaudeCloudGuardError({ reason: "unsupported_policy" });
          }
          if (config.launchArgs.trim())
            throw new ClaudeCloudGuardError({ reason: "api_configuration" });
        });
        const account = yield* fs
          .readFileString(accountPath)
          .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Account))));
        const organizationId = account.oauthAccount.organizationUuid;
        const scope: McpThreadInvocationScope = {
          environmentId: yield* identity.getEnvironmentId,
          capabilities: new Set(["preview"]),
          issuedAt: DateTime.toEpochMillis(yield* DateTime.now),
          requestNamespace: `claude-cloud:${input.providerSessionId}`,
          thread: {
            threadId: input.threadId,
            providerSessionId: `cloud:${input.providerSessionId}`,
            providerInstanceId: instanceId,
          },
          client: undefined,
        };
        const opened = yield* broker.invoke<{ tabId: PreviewTabId | null }>({
          scope,
          operation: "open",
          input: { url: "https://claude.ai/code", open: true, reuseExistingTab: false },
        });
        if (!opened.tabId) return yield* new ClaudeCloudGuardError({ reason: "account_mismatch" });
        const tabId = opened.tabId;

        const browser = (expression: string) =>
          broker.invoke<unknown>({
            scope,
            tabId,
            operation: "evaluate",
            input: { expression, awaitPromise: true },
            timeoutMs: 20_000,
          });

        const request = (endpoint: string, body?: unknown, afterSequence = 0n) =>
          browser(`(async () => {
        if (location.origin !== 'https://claude.ai' || location.pathname.includes('login')) throw new Error('Claude sign-in required');
        const r = await fetch(${json(endpoint)}, {
          method: ${json(body === undefined ? "GET" : "POST")},
          headers: {'anthropic-version':'2023-06-01','anthropic-organization-id':${json(organizationId)},'content-type':'application/json'},
          ${body === undefined ? "" : `body: ${json(json(body))},`}
          signal: AbortSignal.timeout(15000)
        });
        if (!r.ok) throw new Error('Claude cloud HTTP ' + r.status);
        const data = await r.json();
        // Keep old frames, boot prompts, and environment credentials in the browser.
        if (${json(endpoint)}.endsWith('/events') && Array.isArray(data.data)) {
          const allowed = new Set(${json([...relayedTypes])});
          return {data:data.data.filter(e => BigInt(e.sequence_num) > BigInt(${json(afterSequence.toString())})).map(e=>({
            event_id:e.event_id,sequence_num:e.sequence_num,
            payload:allowed.has(e.payload?.type) && !(e.payload?.type === 'system' && e.payload?.subtype === 'init') ? e.payload : {}
          }))};
        }
        const session = data.response_shape ?? data.session;
        if (${json(endpoint)}.startsWith('/v1/code/sessions') && session) {
          return {response_shape:{id:session.id,config:{model:session.config?.model}}};
        }
        return data;
      })()`);

        const guard = Effect.gen(function* () {
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          const usage = yield* request(
            `/api/organizations/${encodeURIComponent(organizationId)}/usage`,
          );
          return yield* Effect.try(() => verifyClaudeCloudCredit(usage, now));
        });
        yield* guard;
        const nativeId = input.options.resume ?? input.options.sessionId;
        if (!nativeId || !/^[a-f0-9-]{36}$/i.test(nativeId)) {
          return yield* new ClaudeCloudGuardError({ reason: "remote_failed" });
        }
        const bindingPath = path.join(bindingDir, `${nativeId}.json`);
        const writeBinding = (value: typeof Binding.Type) =>
          fs
            .writeFileString(`${bindingPath}.tmp`, json(value))
            .pipe(
              Effect.andThen(fs.rename(`${bindingPath}.tmp`, bindingPath)),
              Effect.uninterruptible,
            );
        const exists = yield* fs.exists(bindingPath);
        let binding: typeof Binding.Type;
        if (exists) {
          binding = yield* fs
            .readFileString(bindingPath)
            .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Binding))));
          if (
            binding.organizationId !== organizationId ||
            !binding.cloudId.startsWith("session_")
          ) {
            return yield* new ClaudeCloudGuardError({ reason: "account_mismatch" });
          }
        } else {
          const environments = yield* request(
            `/v1/environment_providers/private/organizations/${encodeURIComponent(organizationId)}/environments`,
          ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(EnvironmentList)));
          const available = environments.environments.filter(
            (e) => e.kind === "anthropic_cloud" && e.state === "active",
          );
          const chosen =
            available.find((e) => e.name === "Default") ??
            (available.length === 1 ? available[0] : undefined);
          if (!chosen) return yield* new ClaudeCloudGuardError({ reason: "remote_failed" });
          const sources: Array<{ type: string; url: string; revision: string }> = [];
          const outcomes: Array<{
            type: string;
            git_info: { type: string; repo: string; branches: string[] };
          }> = [];
          const cwd = input.options.cwd;
          if (cwd) {
            const git = (args: string[]) =>
              processes.run({
                command: "git",
                args,
                cwd,
                timeout: "10 seconds",
                maxOutputBytes: 20_000,
              });
            const root = yield* git(["rev-parse", "--show-toplevel"]);
            if (root.code === 0) {
              const status = yield* git(["status", "--porcelain"]);
              const remote = yield* git(["remote", "get-url", "origin"]);
              const branch = yield* git(["branch", "--show-current"]);
              const upstream = yield* git(["rev-parse", "@{upstream}"]);
              const head = yield* git(["rev-parse", "HEAD"]);
              const published = yield* git([
                "ls-remote",
                "origin",
                `refs/heads/${branch.stdout.trim()}`,
              ]);
              const match =
                /^(?:https:\/\/github\.com\/|git@github\.com:)([\w.-]+\/[\w.-]+?)(?:\.git)?$/.exec(
                  remote.stdout.trim(),
                );
              if (
                status.code !== 0 ||
                status.stdout.trim() ||
                !match ||
                !branch.stdout.trim() ||
                upstream.code !== 0 ||
                upstream.stdout.trim() !== head.stdout.trim() ||
                published.code !== 0 ||
                published.stdout.trim().split(/\s+/)[0] !== head.stdout.trim()
              ) {
                return yield* new ClaudeCloudGuardError({ reason: "repository_not_ready" });
              }
              sources.push({
                type: "git_repository",
                url: `https://github.com/${match[1]}`,
                revision: `refs/heads/${branch.stdout.trim()}`,
              });
              outcomes.push({
                type: "git_repository",
                git_info: {
                  type: "github",
                  repo: match[1]!,
                  branches: [`claude/t3-cloud-${nativeId.slice(0, 12)}`],
                },
              });
            }
          }
          // An uncertain creation is never retried automatically: the first POST may have landed.
          yield* writeBinding({ organizationId, cloudId: "creation-uncertain", sequence: "0" });
          const created = yield* request("/v1/code/sessions", {
            title: "T3 cloud prototype",
            environment_id: chosen.environment_id,
            config: {
              sources,
              outcomes,
              model: CLAUDE_CLOUD_MODEL,
              effort_level: "low",
              permission_mode: "bypassPermissions",
            },
            events: [],
          }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(SessionInfo)));
          binding = {
            organizationId,
            cloudId: created.response_shape.id.replace(/^cse_/, "session_"),
            sequence: "0",
          };
          yield* writeBinding(binding);
        }

        const sessionPath = `/v1/code/sessions/${encodeURIComponent(binding.cloudId)}`;
        const checkModel = request(sessionPath).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(SessionInfo)),
          Effect.flatMap((info) =>
            Effect.try(() => verifyClaudeCloudModel(info.response_shape.config.model)),
          ),
        );
        yield* checkModel;
        yield* broker.invoke({
          scope,
          tabId,
          operation: "navigate",
          input: { url: `https://claude.ai/code/${binding.cloudId}` },
        });
        const queue = yield* Queue.unbounded<
          SDKMessage,
          ClaudeAgentSdkQueryRunnerError | Cause.Done
        >();
        let active = false;
        let lastBillingCheck = 0;
        let sequence = BigInt(binding.sequence);
        const control = (subtype: string, fields: Record<string, unknown> = {}) =>
          Effect.gen(function* () {
            const requestId = yield* crypto.randomUUIDv4();
            yield* request(`${sessionPath}/events`, {
              events: [
                {
                  payload: {
                    type: "control_request",
                    request_id: requestId,
                    uuid: requestId,
                    request: { subtype, ...fields },
                  },
                },
              ],
            });
          });
        const poll = Effect.gen(function* () {
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          if (active && now - lastBillingCheck >= 10_000) {
            yield* guard.pipe(Effect.tapError(() => control("interrupt").pipe(Effect.ignore)));
            lastBillingCheck = now;
          }
          const raw = yield* request(`${sessionPath}/events`, undefined, sequence);
          const events = yield* Effect.try(() => orderedClaudeCloudEvents(raw, sequence));
          const nextSequence = events.at(-1)?.sequence;
          if (nextSequence !== undefined)
            yield* writeBinding({ ...binding, sequence: nextSequence.toString() });
          for (const event of events) {
            const payload = event.payload;
            sequence = event.sequence;
            if (typeof payload.type !== "string" || !relayedTypes.has(payload.type)) continue;
            if (payload.type === "result") active = false;
            // The adapter owns a local UUID; the persisted binding owns the remote session ID.
            yield* Queue.offer(queue, { ...payload, session_id: nativeId } as SDKMessage);
          }
        });
        const fiber = yield* poll.pipe(
          Effect.andThen(Effect.sleep("1 second")),
          Effect.forever,
          Effect.catch((cause) =>
            Effect.gen(function* () {
              if (active) yield* control("interrupt").pipe(Effect.ignore);
              yield* Queue.fail(queue, wrap("events")(cause));
            }),
          ),
          Effect.forkIn(ownerScope),
        );
        const close = Effect.gen(function* () {
          yield* Fiber.interrupt(fiber);
          if (active) yield* control("interrupt").pipe(Effect.ignore);
          yield* Queue.end(queue);
        });
        const runtime: ClaudeAgentSdkQuerySession = {
          messages: Stream.fromQueue(queue),
          offer: (message) =>
            Effect.gen(function* () {
              yield* guard;
              yield* checkModel;
              yield* control("set_permission_mode", { mode: "bypassPermissions" });
              lastBillingCheck = DateTime.toEpochMillis(yield* DateTime.now);
              active = true;
              yield* request(`${sessionPath}/events`, {
                events: [{ payload: { ...message, session_id: binding.cloudId } }],
              });
            }).pipe(Effect.mapError(wrap("offer"))),
          setModel: (model) =>
            Effect.try(() => verifyClaudeCloudModel(model)).pipe(Effect.mapError(wrap("setModel"))),
          applyFlagSettings: (settings) =>
            settings.fastMode ? unsupported("fastMode") : Effect.void,
          setPermissionMode: (mode) =>
            mode === "bypassPermissions" ? Effect.void : unsupported("setPermissionMode"),
          stopTask: () => control("interrupt").pipe(Effect.mapError(wrap("stopTask"))),
          backgroundTasks: () => Effect.succeed(false),
          interrupt: control("interrupt").pipe(Effect.mapError(wrap("interrupt"))),
          close,
        };
        return runtime;
      }).pipe(Effect.mapError(wrap("open"))),
  } satisfies ClaudeAgentSdkQueryRunnerShape;
});
