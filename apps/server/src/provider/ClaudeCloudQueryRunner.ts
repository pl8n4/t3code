import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { ProviderInstanceId, type ClaudeSettings, type PreviewTabId } from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PreviewAutomationBroker from "../mcp/PreviewAutomationBroker.ts";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import { WorkspaceInfo } from "../mcp/AgentWorkspaceSchemas.ts";
import {
  makeClaudeCloudMcpRelay,
  CLAUDE_CLOUD_WORKSPACE_INSTRUCTIONS,
} from "./ClaudeCloudMcpRelay.ts";
import {
  ClaudeAgentSdkQueryRunnerError,
  type ClaudeAgentSdkQueryRunnerShape,
  type ClaudeAgentSdkQueryOpenInput,
  type ClaudeAgentSdkQuerySession,
} from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { resolveClaudeHomePath } from "./Drivers/ClaudeHome.ts";
import {
  ClaudeCloudGuardError,
  ClaudeCloudEvent,
  orderedClaudeCloudEvents,
  verifyClaudeCloudCredit,
  verifyClaudeCloudEnvironment,
  verifyClaudeCloudModel,
} from "./ClaudeCloudPolicy.ts";

const Binding = Schema.Struct({
  organizationId: Schema.String,
  cloudId: Schema.String,
  sequence: Schema.String,
  pendingTurn: Schema.optionalKey(
    Schema.Struct({ resultSequence: Schema.optionalKey(Schema.String) }),
  ),
  inbox: Schema.optionalKey(Schema.Array(ClaudeCloudEvent)),
  toolRequests: Schema.optionalKey(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        state: Schema.Literals(["started", "completed"]),
        response: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
      }),
    ),
  ),
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
const decodeUnknownJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeUnknownRecord = Schema.decodeUnknownEffect(
  Schema.Record(Schema.String, Schema.Unknown),
);
const decodeWorkspaceInfo = Schema.decodeUnknownEffect(WorkspaceInfo);

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
  "control_request",
  "control_response",
  "control_cancel_request",
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
  if (Option.isNone(maybeBroker) || Option.isNone(maybeIdentity)) {
    return yield* new ClaudeCloudGuardError({ reason: "account_mismatch" });
  }
  const broker = maybeBroker.value;
  const identity = maybeIdentity.value;
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
        const queryScope = yield* Scope.fork(ownerScope);
        return yield* Effect.gen(function* () {
          let selectedModel = yield* Effect.try(() => verifyClaudeCloudModel(input.options.model));
          yield* Effect.try(() => {
            verifyClaudeCloudEnvironment(environment);
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
            input: { url: "https://claude.ai/new", open: false, reuseExistingTab: false },
          });
          if (!opened.tabId)
            return yield* new ClaudeCloudGuardError({ reason: "account_mismatch" });
          const tabId = opened.tabId;

          const browser = (expression: string) =>
            broker.invoke<unknown>({
              scope,
              tabId,
              operation: "evaluate",
              input: { expression, awaitPromise: true },
              timeoutMs: 20_000,
            });

          const requestOnce = (endpoint: string, body?: unknown, afterSequence = 0n) =>
            Effect.gen(function* () {
              const key = yield* crypto.randomUUIDv4();
              const encoded = body === undefined ? undefined : json(body);
              const inlineBody = encoded !== undefined && Buffer.byteLength(encoded) <= 10000;
              if (encoded !== undefined && !inlineBody) {
                for (let offset = 0; offset < encoded.length; offset += 24000) {
                  yield* browser(
                    `(() => {const b=globalThis.__t3CloudBuffers??=Object.create(null);(b[${json(key)}]??=[]).push(${json(encoded.slice(offset, offset + 24000))});return null})()`,
                  );
                }
              }
              const result = yield* browser(`(async () => {
            if (location.origin !== 'https://claude.ai' || location.pathname.includes('login')) throw new Error('Claude sign-in required');
            const buffers=globalThis.__t3CloudBuffers??=Object.create(null);
            let r;
            try {r=await fetch(${json(endpoint)}, {
              method: ${json(body === undefined ? "GET" : "POST")},
              headers: {'anthropic-version':'2023-06-01','anthropic-organization-id':${json(organizationId)},'content-type':'application/json'},
              ${encoded === undefined ? "" : `body:${inlineBody ? json(encoded) : `buffers[${json(key)}].join('')`},`}
              signal: AbortSignal.timeout(15000)
            });}finally{delete buffers[${json(key)}]}
            if (!r.ok) throw new Error('Claude cloud HTTP ' + r.status);
            if(r.status===204)return null;
            const data=await r.json();
            if (${json(endpoint)}.endsWith('/events') && Array.isArray(data.data)) {
              const allowed=new Set(${json([...relayedTypes])});
              const page={data:data.data.filter(e=>BigInt(e.sequence_num)>BigInt(${json(afterSequence.toString())})).map(e=>({
                event_id:e.event_id,sequence_num:e.sequence_num,
                payload:allowed.has(e.payload?.type) && !(e.payload?.type==='system' && e.payload?.subtype==='init')?e.payload:{}
              }))};
              const value=new TextEncoder().encode(JSON.stringify(page));
              if(value.length<=24000)return page;
              if(value.length>16000000)throw new Error('Cloud event page too large');
              buffers[${json(key)}]=value;
              return {cloudBuffer:${json(key)},length:value.length};
            }
            const session=data.response_shape??data.session;
            if (${json(endpoint)}.startsWith('/v1/code/sessions') && session)
              return {response_shape:{id:session.id,config:{model:session.config?.model}}};
            return data;
          })()`);
              if (
                typeof result === "object" &&
                result !== null &&
                "cloudBuffer" in result &&
                "length" in result &&
                typeof result.length === "number"
              ) {
                const chunks: Buffer[] = [];
                for (let offset = 0; offset < result.length; offset += 24000) {
                  const chunk = yield* browser(
                    `btoa(String.fromCharCode(...globalThis.__t3CloudBuffers[${json(key)}].slice(${offset},${offset + 24000})))`,
                  );
                  if (typeof chunk !== "string")
                    return yield* new ClaudeCloudGuardError({ reason: "remote_failed" });
                  chunks.push(Buffer.from(chunk, "base64"));
                }
                yield* browser(`delete globalThis.__t3CloudBuffers[${json(key)}]`);
                return yield* decodeUnknownJson(Buffer.concat(chunks).toString("utf8"));
              }
              return result;
            });
          const request = (endpoint: string, body?: unknown, afterSequence = 0n) => {
            const call = requestOnce(endpoint, body, afterSequence);
            return body === undefined
              ? call.pipe(Effect.retry({ times: 2, schedule: Schedule.exponential("250 millis") }))
              : call;
          };
          const relay = yield* makeClaudeCloudMcpRelay(input.options.mcpServers);
          if (input.options.cwd && !relay)
            return yield* new ClaudeCloudGuardError({ reason: "tools_unavailable" });

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
            if (input.options.resume)
              return yield* new ClaudeCloudGuardError({ reason: "session_missing" });
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
            // An uncertain creation is never retried automatically: the first POST may have landed.
            yield* writeBinding({ organizationId, cloudId: "creation-uncertain", sequence: "0" });
            const created = yield* request("/v1/code/sessions", {
              title: "T3 cloud prototype",
              environment_id: chosen.environment_id,
              config: {
                sources: [],
                outcomes: [],
                append_system_prompt: CLAUDE_CLOUD_WORKSPACE_INSTRUCTIONS,
                model: selectedModel,
                effort_level: "low",
                permission_mode: "bypassPermissions",
              },
              events: [],
            }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(SessionInfo)));
            binding = {
              organizationId,
              cloudId: created.response_shape.id.replace(/^cse_/, "session_"),
              sequence: "0",
              inbox: [],
              toolRequests: {},
            };
            yield* writeBinding(binding);
          }

          const bindingLock = yield* Semaphore.make(1);
          const updateBinding = (update: (current: typeof Binding.Type) => typeof Binding.Type) =>
            bindingLock.withPermits(1)(
              Effect.gen(function* () {
                const next = update(binding);
                yield* writeBinding(next);
                binding = next;
              }).pipe(Effect.uninterruptible),
            );
          const acknowledge = (checkpoint: string | undefined) => {
            if (checkpoint === undefined) return Effect.void;
            return Effect.gen(function* () {
              const cursor = BigInt(checkpoint);
              if (cursor > BigInt(binding.sequence))
                return yield* new ClaudeCloudGuardError({ reason: "remote_failed" });
              yield* updateBinding((current) => {
                const { pendingTurn, ...rest } = current;
                return {
                  ...rest,
                  ...(pendingTurn !== undefined &&
                  (pendingTurn.resultSequence === undefined ||
                    BigInt(pendingTurn.resultSequence) > cursor)
                    ? { pendingTurn }
                    : {}),
                  inbox: (current.inbox ?? []).filter(
                    (event) => BigInt(event.sequence_num) > cursor,
                  ),
                };
              });
            });
          };
          yield* acknowledge(input.durableCloudSequence);

          const sessionPath = `/v1/code/sessions/${encodeURIComponent(binding.cloudId)}`;
          const checkModel = request(sessionPath).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(SessionInfo)),
            Effect.flatMap((info) =>
              Effect.try(() => verifyClaudeCloudModel(info.response_shape.config.model)),
            ),
          );
          yield* checkModel;
          let usable = true;
          let recoveryPending = false;
          const requireRecovery = Effect.gen(function* () {
            // Admission must stop before recovery waits on the remote interrupt or browser.
            recoveryPending = true;
            usable = false;
            const requestId = yield* crypto.randomUUIDv4();
            yield* request(`${sessionPath}/events`, {
              events: [
                {
                  payload: {
                    type: "control_request",
                    request_id: requestId,
                    uuid: requestId,
                    request: { subtype: "interrupt" },
                  },
                },
              ],
            }).pipe(Effect.ignore);
            yield* broker
              .invoke({
                scope,
                tabId,
                operation: "navigate",
                input: { url: `https://claude.ai/code/${binding.cloudId}` },
              })
              .pipe(Effect.ignore);
            return yield* new ClaudeCloudGuardError({ reason: "delivery_pending" });
          });
          if (
            binding.pendingTurn !== undefined ||
            (binding.inbox ?? []).some((event) =>
              ["assistant", "user", "result", "stream_event"].includes(String(event.payload.type)),
            )
          )
            return yield* requireRecovery;
          const queue = yield* Queue.unbounded<
            SDKMessage,
            ClaudeAgentSdkQueryRunnerError | Cause.Done
          >();
          let active = false;
          let closed = false;
          let connecting = relay !== undefined;
          let lastBillingCheck = 0;
          let sequence = BigInt(binding.sequence);
          const sdkSequences = new WeakMap<SDKMessage, string>();
          let replay = binding.inbox ?? [];
          const pendingControls = new Map<
            string,
            Deferred.Deferred<unknown, ClaudeCloudGuardError>
          >();
          const toolJobs = new Map<string, Fiber.Fiber<void, never>>();
          const completedControls = new Set<string>();
          const pollWake = yield* Queue.unbounded<void>();
          const mcpReady = yield* Deferred.make<void, ClaudeCloudGuardError>();
          const initialPoll = yield* Deferred.make<void, ClaudeCloudGuardError>();
          const admission = yield* Semaphore.make(1);
          const control = (subtype: string, fields: Record<string, unknown> = {}, wait = false) =>
            Effect.gen(function* () {
              const requestId = yield* crypto.randomUUIDv4();
              const reply = yield* Deferred.make<unknown, ClaudeCloudGuardError>();
              if (wait) pendingControls.set(requestId, reply);
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
              yield* Queue.offer(pollWake, undefined);
              if (!wait) return;
              return yield* Deferred.await(reply).pipe(
                Effect.timeout("90 seconds"),
                Effect.ensuring(Effect.sync(() => pendingControls.delete(requestId))),
              );
            });
          const replyToTool = (payload: Record<string, unknown>) =>
            Effect.gen(function* () {
              const requestId = payload.request_id;
              const decoded = yield* decodeUnknownRecord(payload.request);
              if (
                typeof requestId !== "string" ||
                decoded.subtype !== "mcp_message" ||
                decoded.server_name !== "t3-code" ||
                !relay ||
                completedControls.has(requestId) ||
                toolJobs.has(requestId)
              )
                return;
              const saved = binding.toolRequests?.[requestId];
              const method =
                typeof decoded.message === "object" &&
                decoded.message !== null &&
                "method" in decoded.message
                  ? decoded.message.method
                  : undefined;
              if (
                !active &&
                !saved &&
                !["initialize", "notifications/initialized", "tools/list"].includes(String(method))
              ) {
                return yield* requireRecovery;
              }
              const job = yield* Effect.gen(function* () {
                const saved = binding.toolRequests?.[requestId];
                if (saved?.state === "completed" && saved.response) return saved.response;
                if (saved)
                  return {
                    subtype: "error",
                    request_id: requestId,
                    error:
                      "A previous attempt may have executed this tool. It was not repeated. Inspect the local workspace before retrying.",
                  };
                yield* updateBinding((current) => ({
                  ...current,
                  toolRequests: {
                    ...current.toolRequests,
                    [requestId]: { state: "started" },
                  },
                }));
                const response = yield* relay(decoded.message).pipe(
                  Effect.match({
                    onSuccess: (mcp_response) => ({
                      subtype: "success",
                      request_id: requestId,
                      response: { mcp_response },
                    }),
                    onFailure: () => ({
                      subtype: "error",
                      request_id: requestId,
                      error:
                        "T3 tool connection failed. The request was not retried; inspect the local workspace before retrying.",
                    }),
                  }),
                );
                yield* updateBinding((current) => ({
                  ...current,
                  toolRequests: {
                    ...current.toolRequests,
                    [requestId]: { state: "completed", response },
                  },
                }));
                return response;
              }).pipe(
                Effect.flatMap((response) =>
                  request(`${sessionPath}/events`, {
                    events: [{ payload: { type: "control_response", uuid: requestId, response } }],
                  }).pipe(
                    Effect.retry({ times: 2, schedule: Schedule.exponential("250 millis") }),
                    Effect.as(response),
                  ),
                ),
                Effect.tap(() => Effect.sync(() => completedControls.add(requestId))),
                Effect.tap(() => Queue.offer(pollWake, undefined)),
                Effect.tap((response) =>
                  typeof decoded.message === "object" &&
                  decoded.message !== null &&
                  "method" in decoded.message &&
                  decoded.message.method === "tools/list"
                    ? response.subtype === "success"
                      ? Deferred.succeed(mcpReady, undefined)
                      : Deferred.fail(
                          mcpReady,
                          new ClaudeCloudGuardError({ reason: "tools_unavailable" }),
                        )
                    : Effect.void,
                ),
                Effect.asVoid,
                Effect.catch((cause) =>
                  Effect.gen(function* () {
                    yield* Deferred.fail(
                      mcpReady,
                      new ClaudeCloudGuardError({ reason: "tools_unavailable" }),
                    );
                    yield* Queue.fail(queue, wrap("tool-response")(cause));
                  }),
                ),
                Effect.ensuring(Effect.sync(() => toolJobs.delete(requestId))),
                Effect.forkIn(queryScope),
              );
              toolJobs.set(requestId, job);
            });
          const poll = Effect.gen(function* () {
            const now = DateTime.toEpochMillis(yield* DateTime.now);
            if (active && now - lastBillingCheck >= 10_000) {
              yield* guard.pipe(Effect.tapError(() => control("interrupt").pipe(Effect.ignore)));
              lastBillingCheck = now;
            }
            const raw = yield* request(`${sessionPath}/events`, undefined, sequence);
            const fetched = yield* Effect.try(() => orderedClaudeCloudEvents(raw, sequence));
            const nextSequence = fetched.at(-1)?.sequence;
            if (nextSequence !== undefined)
              yield* updateBinding((current) => ({
                ...current,
                sequence: nextSequence.toString(),
                inbox: [
                  ...(current.inbox ?? []),
                  ...fetched.map(({ event_id, sequence_num, payload }) => ({
                    event_id,
                    sequence_num,
                    payload,
                  })),
                ],
              }));
            const events = [...replay, ...fetched];
            replay = [];
            if (nextSequence !== undefined) sequence = nextSequence;
            for (const event of events) {
              const payload = event.payload;
              if (
                !active &&
                ["assistant", "user", "result", "stream_event"].includes(String(payload.type))
              ) {
                return yield* requireRecovery;
              }
              if (payload.type === "control_request") {
                yield* replyToTool(payload);
                continue;
              }
              if (
                payload.type === "control_cancel_request" &&
                typeof payload.request_id === "string"
              ) {
                const job = toolJobs.get(payload.request_id);
                if (job) yield* Fiber.interrupt(job);
                continue;
              }
              if (payload.type === "control_response") {
                const response = payload.response;
                if (
                  typeof response === "object" &&
                  response !== null &&
                  "request_id" in response &&
                  typeof response.request_id === "string"
                ) {
                  const deferred = pendingControls.get(response.request_id);
                  if (deferred) {
                    if ("subtype" in response && response.subtype === "success")
                      yield* Deferred.succeed(
                        deferred,
                        "response" in response ? response.response : {},
                      );
                    else
                      yield* Deferred.fail(
                        deferred,
                        new ClaudeCloudGuardError({ reason: "tools_unavailable" }),
                      );
                  }
                }
                continue;
              }
              if (typeof payload.type !== "string" || !relayedTypes.has(payload.type)) continue;
              if (payload.type === "result") {
                yield* updateBinding((current) => ({
                  ...current,
                  pendingTurn: { resultSequence: event.sequence_num },
                }));
                active = false;
              }
              // The adapter owns a local UUID; the persisted binding owns the remote session ID.
              const message = { ...payload, session_id: nativeId } as SDKMessage;
              sdkSequences.set(message, event.sequence_num);
              yield* Queue.offer(queue, message);
            }
          });
          const fiber = yield* Effect.gen(function* () {
            yield* poll;
            yield* Deferred.succeed(initialPoll, undefined);
            yield* Queue.take(pollWake).pipe(
              Effect.timeoutOption(
                active || connecting || pendingControls.size > 0 ? "200 millis" : "2 seconds",
              ),
            );
          }).pipe(
            Effect.forever,
            Effect.catch((cause) =>
              Effect.gen(function* () {
                usable = false;
                if (active) yield* control("interrupt").pipe(Effect.ignore);
                const error = new ClaudeCloudGuardError({
                  reason: recoveryPending ? "delivery_pending" : "remote_failed",
                });
                yield* Deferred.fail(initialPoll, error);
                yield* Deferred.fail(mcpReady, error);
                for (const deferred of pendingControls.values())
                  yield* Deferred.fail(deferred, error);
                yield* Queue.fail(queue, wrap("events")(cause));
              }),
            ),
            Effect.forkIn(queryScope),
          );
          yield* Scope.addFinalizer(
            queryScope,
            Effect.gen(function* () {
              closed = true;
              yield* Fiber.interrupt(fiber);
              yield* Effect.forEach(toolJobs.values(), Fiber.interrupt, { discard: true });
              if (active) yield* control("interrupt").pipe(Effect.ignore);
              yield* Queue.end(queue);
            }),
          );
          const close = Scope.close(queryScope, Exit.void);
          yield* Deferred.await(initialPoll);
          if (relay) {
            const status = yield* control("mcp_status", {}, true);
            const servers: Record<string, unknown> = {};
            if (
              typeof status === "object" &&
              status !== null &&
              "mcpServers" in status &&
              Array.isArray(status.mcpServers)
            ) {
              for (const entry of status.mcpServers) {
                if (
                  typeof entry?.name === "string" &&
                  entry.config &&
                  (entry.scope === "dynamic" || entry.source === "sdk")
                )
                  servers[entry.name] = entry.config;
              }
            }
            servers["t3-code"] = { type: "sdk", name: "t3-code", timeout: 90000 };
            const registered = yield* control("mcp_set_servers", { servers }, true);
            if (
              typeof registered === "object" &&
              registered !== null &&
              "errors" in registered &&
              typeof registered.errors === "object" &&
              registered.errors !== null &&
              "t3-code" in registered.errors
            )
              return yield* new ClaudeCloudGuardError({ reason: "tools_unavailable" });
            const liveStatus = yield* control("mcp_status", {}, true);
            const connected =
              typeof liveStatus === "object" &&
              liveStatus !== null &&
              "mcpServers" in liveStatus &&
              Array.isArray(liveStatus.mcpServers) &&
              liveStatus.mcpServers.some((s) => s.name === "t3-code" && s.status === "connected");
            if (!connected) yield* Deferred.await(mcpReady).pipe(Effect.timeout("90 seconds"));
          }
          connecting = false;
          if (!usable || recoveryPending)
            return yield* new ClaudeCloudGuardError({
              reason: recoveryPending ? "delivery_pending" : "remote_failed",
            });
          const runtime: ClaudeAgentSdkQuerySession = {
            messages: Stream.fromQueue(queue),
            cloudEventSequence: (message) => sdkSequences.get(message),
            offer: (message, durableCloudSequence) =>
              Effect.gen(function* () {
                if (closed || !usable || recoveryPending)
                  return yield* new ClaudeCloudGuardError({
                    reason: recoveryPending ? "delivery_pending" : "remote_failed",
                  });
                yield* acknowledge(durableCloudSequence);
                if (binding.pendingTurn !== undefined)
                  return yield* new ClaudeCloudGuardError({ reason: "delivery_pending" });
                yield* Effect.all([guard, checkModel], { concurrency: "unbounded" });
                // Reassert the explicit selection on resumed sessions as well as fresh ones.
                yield* control("set_model", { model: selectedModel }, true).pipe(
                  Effect.mapError(
                    () =>
                      new ClaudeCloudGuardError({
                        reason: recoveryPending ? "delivery_pending" : "model_unavailable",
                      }),
                  ),
                );
                if (relay) {
                  yield* control(
                    "apply_flag_settings",
                    {
                      settings: {
                        permissions: {
                          allow: ["mcp__t3-code__*"],
                          deny: [
                            "Read",
                            "Write",
                            "Edit",
                            "MultiEdit",
                            "Glob",
                            "Grep",
                            "Bash",
                            "Agent",
                            "Task",
                          ],
                        },
                      },
                    },
                    true,
                  );
                  yield* control("set_permission_mode", { mode: "dontAsk" }, true);
                } else yield* control("set_permission_mode", { mode: "bypassPermissions" });
                let forwarded = message;
                if (relay) {
                  const workspace = yield* relay({
                    jsonrpc: "2.0",
                    id: yield* crypto.randomUUIDv4(),
                    method: "tools/call",
                    params: { name: "t3_workspace_info", arguments: {} },
                  });
                  if (
                    typeof workspace !== "object" ||
                    workspace === null ||
                    !("result" in workspace) ||
                    typeof workspace.result !== "object" ||
                    workspace.result === null ||
                    ("isError" in workspace.result && workspace.result.isError === true)
                  )
                    return yield* new ClaudeCloudGuardError({ reason: "tools_unavailable" });
                  const toolResult = workspace.result;
                  const info = yield* Effect.gen(function* () {
                    if ("structuredContent" in toolResult)
                      return yield* decodeWorkspaceInfo(toolResult.structuredContent);
                    const contents =
                      "content" in toolResult && Array.isArray(toolResult.content)
                        ? toolResult.content
                        : [];
                    const text = contents.find((c) => c?.type === "text")?.text;
                    return yield* decodeWorkspaceInfo(yield* decodeUnknownJson(text));
                  }).pipe(
                    Effect.mapError(
                      () => new ClaudeCloudGuardError({ reason: "tools_unavailable" }),
                    ),
                  );
                  if (
                    input.options.cwd &&
                    path.resolve(info.cwd) !== path.resolve(input.options.cwd)
                  )
                    return yield* new ClaudeCloudGuardError({ reason: "tools_unavailable" });
                  const context = `<t3_workspace_context>\n${CLAUDE_CLOUD_WORKSPACE_INSTRUCTIONS}\nCurrent T3 workspace: ${json(info)}\nFor repository and PR questions, use mcp__t3-code__t3_workspace_pull_requests and t3_workspace_pull_request_read. For git, gh, files and tests, use mcp__t3-code__t3_workspace_exec. These tools already have the local repository and source-control login. Do not try cloud Bash or conclude the project is missing because /home/user is empty. Read the workspace's AGENTS.md or CLAUDE.md before changing source files.\n</t3_workspace_context>`;
                  const content = message.message.content;
                  forwarded = {
                    ...message,
                    message: {
                      ...message.message,
                      content: [
                        { type: "text", text: context },
                        ...(typeof content === "string"
                          ? [{ type: "text" as const, text: content }]
                          : content),
                      ],
                    },
                  };
                }
                lastBillingCheck = DateTime.toEpochMillis(yield* DateTime.now);
                // Persist admission before the POST: a disconnect can precede the first event poll.
                yield* updateBinding((current) => ({ ...current, pendingTurn: {} }));
                if (closed || !usable || recoveryPending)
                  return yield* new ClaudeCloudGuardError({
                    reason: recoveryPending ? "delivery_pending" : "remote_failed",
                  });
                active = true;
                yield* request(`${sessionPath}/events`, {
                  events: [{ payload: { ...forwarded, session_id: binding.cloudId } }],
                }).pipe(Effect.tapError(() => control("interrupt").pipe(Effect.ignore)));
                yield* Queue.offer(pollWake, undefined);
              }).pipe(admission.withPermits(1), Effect.mapError(wrap("offer"))),
            setModel: (model) =>
              Effect.gen(function* () {
                const nextModel = yield* Effect.try(() => verifyClaudeCloudModel(model));
                if (closed || !usable || recoveryPending || active)
                  return yield* new ClaudeCloudGuardError({
                    reason: recoveryPending ? "delivery_pending" : "unsupported_policy",
                  });
                yield* Effect.all([guard, checkModel], { concurrency: "unbounded" });
                yield* control("set_model", { model: nextModel }, true).pipe(
                  Effect.mapError(
                    () =>
                      new ClaudeCloudGuardError({
                        reason: recoveryPending ? "delivery_pending" : "model_unavailable",
                      }),
                  ),
                );
                if (closed || !usable || recoveryPending)
                  return yield* new ClaudeCloudGuardError({
                    reason: recoveryPending ? "delivery_pending" : "remote_failed",
                  });
                selectedModel = nextModel;
              }).pipe(admission.withPermits(1), Effect.mapError(wrap("setModel"))),
            applyFlagSettings: (settings) =>
              settings.fastMode ? unsupported("fastMode") : Effect.void,
            setPermissionMode: (mode) =>
              mode === "bypassPermissions" ? Effect.void : unsupported("setPermissionMode"),
            stopTask: () =>
              control("interrupt").pipe(Effect.asVoid, Effect.mapError(wrap("stopTask"))),
            backgroundTasks: () => Effect.succeed(false),
            interrupt: control("interrupt").pipe(Effect.asVoid, Effect.mapError(wrap("interrupt"))),
            close,
          };
          return runtime;
        }).pipe(Effect.onError(() => Scope.close(queryScope, Exit.void)));
      }).pipe(Effect.mapError(wrap("open"))),
  } satisfies ClaudeAgentSdkQueryRunnerShape;
});
