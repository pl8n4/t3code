import type { SDKMessage, SDKUserMessage, EffortLevel } from "@anthropic-ai/claude-agent-sdk";
import {
  DEFAULT_BROWSER_PROFILE_ID,
  ProviderInstanceId,
  type ClaudeSettings,
  type PreviewTabId,
} from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Hex from "effect/encoding/Hex";
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
    Schema.Struct({
      promptUuid: Schema.optionalKey(Schema.String),
      replayAfterSequence: Schema.optionalKey(Schema.String),
      resultSequence: Schema.optionalKey(Schema.String),
    }),
  ),
  inbox: Schema.optionalKey(Schema.Array(ClaudeCloudEvent)),
  toolRequests: Schema.optionalKey(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        state: Schema.Literals(["started", "completed"]),
        commandRequestId: Schema.optionalKey(
          Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(200)),
        ),
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
const HttpFailure = Schema.Struct({ cloudHttpStatus: Schema.Number });
const isHttpFailure = Schema.is(HttpFailure);
class ClaudeCloudHttpError extends Schema.TaggedError<ClaudeCloudHttpError>()(
  "ClaudeCloudHttpError",
  { status: Schema.Number },
) {
  override get message(): string {
    return `Claude cloud returned HTTP ${this.status}.`;
  }
}
const decodeAppliedReasoning = Schema.decodeUnknownEffect(
  Schema.Struct({
    applied: Schema.Struct({
      effort: Schema.NullOr(Schema.Literals(["low", "medium", "high", "xhigh", "max"])),
    }),
  }),
);

const isClaudeCloudHttpError = Schema.is(ClaudeCloudHttpError);
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
  yield* fs.makeDirectory(bindingDir, { recursive: true, mode: 0o700 });

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
          let selectedEffort: EffortLevel | null = input.options.effort ?? null;
          const initialSettings =
            typeof input.options.settings === "object" ? input.options.settings : undefined;
          let showThinkingSummaries =
            initialSettings?.showThinkingSummaries ?? input.options.thinking?.type !== "disabled";
          yield* Effect.try(() => {
            verifyClaudeCloudEnvironment(environment);
            if (input.options.permissionMode !== "bypassPermissions") {
              throw new ClaudeCloudGuardError({ reason: "unsupported_policy" });
            }
            if (config.launchArgs.trim())
              throw new ClaudeCloudGuardError({ reason: "api_configuration" });
            if (initialSettings?.fastMode || initialSettings?.ultracode)
              throw new ClaudeCloudGuardError({ reason: "unsupported_policy" });
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
            // Startup recovery can run before any client reports browser profiles. An
            // implicit profile would create throwaway storage and lose the Claude login.
            input: {
              url: "https://claude.ai/new",
              open: false,
              reuseExistingTab: false,
              profileId: DEFAULT_BROWSER_PROFILE_ID,
            },
          });
          if (!opened.tabId)
            return yield* new ClaudeCloudGuardError({ reason: "account_mismatch" });
          let tabId = opened.tabId;

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
            if (!r.ok) return {cloudHttpStatus:r.status};
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
              if (isHttpFailure(result))
                return yield* new ClaudeCloudHttpError({ status: result.cloudHttpStatus });
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
            let reopenedForRequest = false;
            return body === undefined
              ? call.pipe(
                  Effect.retry({
                    times: 2,
                    schedule: Schedule.exponential("250 millis"),
                    while: (error) => !isClaudeCloudHttpError(error) || error.status >= 500,
                  }),
                  Effect.tapError(() =>
                    Effect.suspend(() => {
                      if (reopenedForRequest) return Effect.void;
                      reopenedForRequest = true;
                      return broker
                        .invoke<{ tabId: PreviewTabId | null }>({
                          scope,
                          operation: "open",
                          input: {
                            url: "https://claude.ai/new",
                            open: false,
                            tabId,
                            reuseExistingTab: true,
                            profileId: DEFAULT_BROWSER_PROFILE_ID,
                          },
                        })
                        .pipe(
                          Effect.catch(() =>
                            broker.invoke<{ tabId: PreviewTabId | null }>({
                              scope,
                              operation: "open",
                              input: {
                                url: "https://claude.ai/new",
                                open: false,
                                reuseExistingTab: false,
                                profileId: DEFAULT_BROWSER_PROFILE_ID,
                              },
                            }),
                          ),
                          Effect.tap((reopened) =>
                            Effect.sync(() => {
                              if (reopened.tabId !== null) tabId = reopened.tabId;
                            }),
                          ),
                          Effect.ignore,
                        );
                    }),
                  ),
                  Effect.retry({
                    times: 20,
                    schedule: Schedule.spaced("2 seconds"),
                    while: (error) => !isClaudeCloudHttpError(error) || error.status >= 500,
                  }),
                )
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
              .writeFileString(`${bindingPath}.tmp`, json(value), { mode: 0o600 })
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
                ...(selectedEffort === null ? {} : { effort_level: selectedEffort }),
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
                const unresolved =
                  pendingTurn !== undefined &&
                  (pendingTurn.resultSequence === undefined ||
                    BigInt(pendingTurn.resultSequence) > cursor);
                // Rebuild incomplete turns from their beginning after a process restart. An
                // artifact checkpoint alone cannot reconstruct stream and tool adapter state.
                const pruneThrough =
                  unresolved && pendingTurn.replayAfterSequence !== undefined
                    ? BigInt(pendingTurn.replayAfterSequence)
                    : cursor;
                return {
                  ...rest,
                  ...(unresolved ? { pendingTurn } : {}),
                  inbox: (current.inbox ?? []).filter(
                    (event) => BigInt(event.sequence_num) > pruneThrough,
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
          const recoveringTurn =
            input.recoverPendingCloudTurn === true &&
            binding.pendingTurn?.promptUuid !== undefined &&
            binding.pendingTurn.replayAfterSequence !== undefined
              ? { promptUuid: binding.pendingTurn.promptUuid }
              : undefined;
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
            (binding.pendingTurn !== undefined && recoveringTurn === undefined) ||
            (recoveringTurn === undefined &&
              (binding.inbox ?? []).some((event) =>
                ["assistant", "user", "result", "stream_event"].includes(
                  String(event.payload.type),
                ),
              ))
          )
            return yield* requireRecovery;
          const queue = yield* Queue.unbounded<
            SDKMessage,
            ClaudeAgentSdkQueryRunnerError | Cause.Done
          >();
          let active = false;
          let recoveryPaused = recoveringTurn !== undefined;
          let closed = false;
          let connecting = relay !== undefined;
          let lastBillingCheck = 0;
          let sequence = BigInt(binding.sequence);
          const sdkSequences = new WeakMap<SDKMessage, string>();
          let replay = binding.inbox ?? [];
          const recoveryBacklog: Array<typeof ClaudeCloudEvent.Type> = [];
          const pendingControls = new Map<
            string,
            Deferred.Deferred<unknown, ClaudeCloudGuardError>
          >();
          const toolJobs = new Map<string, Fiber.Fiber<void, never>>();
          const completedControls = new Set<string>();
          const pollWake = yield* Queue.unbounded<void>();
          let mcpReady = yield* Deferred.make<void, ClaudeCloudGuardError>();
          const initialPoll = yield* Deferred.make<void, ClaudeCloudGuardError>();
          const admission = yield* Semaphore.make(1);
          const replayLock = yield* Semaphore.make(1);
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
              const mcpMessage =
                typeof decoded.message === "object" && decoded.message !== null
                  ? decoded.message
                  : undefined;
              const method =
                mcpMessage !== undefined && "method" in mcpMessage ? mcpMessage.method : undefined;
              const params =
                mcpMessage !== undefined &&
                "params" in mcpMessage &&
                typeof mcpMessage.params === "object" &&
                mcpMessage.params !== null
                  ? mcpMessage.params
                  : undefined;
              const resumableCommand =
                method === "tools/call" &&
                params !== undefined &&
                "name" in params &&
                params.name === "t3_workspace_exec";
              const toolArguments =
                params !== undefined &&
                "arguments" in params &&
                typeof params.arguments === "object" &&
                params.arguments !== null
                  ? params.arguments
                  : {};
              let commandRequestId: string | undefined;
              if (resumableCommand) {
                const clientKey =
                  "requestId" in toolArguments ? toolArguments.requestId : undefined;
                const legacyKey = `cloud:${binding.cloudId}:${requestId}`;
                // Older started receipts already used the literal control key. Preserve it
                // so upgrading cannot turn recovery into a second execution of that command.
                commandRequestId =
                  saved?.commandRequestId ??
                  (saved?.state === "started" && legacyKey.length <= 200
                    ? legacyKey
                    : `cloud:${Hex.encode(
                        yield* crypto.digest(
                          "SHA-256",
                          new TextEncoder().encode(
                            json([
                              binding.cloudId,
                              typeof clientKey === "string" ? "client" : "control",
                              typeof clientKey === "string" ? clientKey : requestId,
                            ]),
                          ),
                        ),
                      )}`);
              }
              const relayedMessage = resumableCommand
                ? {
                    ...mcpMessage,
                    params: {
                      ...params,
                      arguments: {
                        ...toolArguments,
                        // The local job service rejects mismatched reuse and returns the
                        // original job instead of launching a command twice.
                        requestId: commandRequestId,
                      },
                    },
                  }
                : decoded.message;
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
                if (saved && !resumableCommand)
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
                    [requestId]: {
                      state: "started",
                      ...(commandRequestId === undefined ? {} : { commandRequestId }),
                    },
                  },
                }));
                const response = yield* relay(relayedMessage).pipe(
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
                    [requestId]: {
                      state: "completed",
                      response,
                      ...(commandRequestId === undefined ? {} : { commandRequestId }),
                    },
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
              const remoteRequest =
                typeof payload.request === "object" && payload.request !== null
                  ? payload.request
                  : undefined;
              const remoteMessage =
                remoteRequest !== undefined &&
                "message" in remoteRequest &&
                typeof remoteRequest.message === "object" &&
                remoteRequest.message !== null
                  ? remoteRequest.message
                  : undefined;
              const initializationRequest =
                payload.type === "control_request" &&
                remoteMessage !== undefined &&
                "method" in remoteMessage &&
                ["initialize", "notifications/initialized", "tools/list"].includes(
                  String(remoteMessage.method),
                );
              if (recoveryPaused && payload.type !== "control_response" && !initializationRequest) {
                recoveryBacklog.push(event);
                continue;
              }
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
                  pendingTurn: { ...current.pendingTurn, resultSequence: event.sequence_num },
                }));
                active = false;
              }
              // The adapter owns a local UUID; the persisted binding owns the remote session ID.
              const message = { ...payload, session_id: nativeId } as SDKMessage;
              sdkSequences.set(message, event.sequence_num);
              yield* Queue.offer(queue, message);
            }
          }).pipe(replayLock.withPermits(1));
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
          const ensureMcpConnected = Effect.gen(function* () {
            if (!relay) return;
            const status = yield* control("mcp_status", {}, true);
            const servers: Record<string, unknown> = {};
            let reconnect = false;
            if (
              typeof status === "object" &&
              status !== null &&
              "mcpServers" in status &&
              Array.isArray(status.mcpServers)
            ) {
              for (const entry of status.mcpServers) {
                if (entry?.name === "t3-code" && entry.source === "sdk") {
                  if (entry.status === "connected") return;
                  reconnect = entry.status === "failed" || entry.status === "disconnected";
                }
                if (
                  typeof entry?.name === "string" &&
                  entry.config &&
                  (entry.scope === "dynamic" ||
                    entry.source === "dynamic" ||
                    entry.source === "sdk")
                )
                  servers[entry.name] = entry.config;
              }
            }
            // Cloud workers can be recycled while idle. Readiness from the old worker
            // must never admit a prompt before this worker has its workspace tools.
            mcpReady = yield* Deferred.make<void, ClaudeCloudGuardError>();
            servers["t3-code"] = { type: "sdk", name: "t3-code", timeout: 90000 };
            const registered = reconnect
              ? yield* control("mcp_reconnect", { serverName: "t3-code" }, true)
              : yield* control("mcp_set_servers", { servers }, true);
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
              liveStatus.mcpServers.some(
                (s) => s.name === "t3-code" && s.source === "sdk" && s.status === "connected",
              );
            if (!connected) {
              yield* Deferred.await(mcpReady).pipe(Effect.timeout("90 seconds"));
              const verified = yield* control("mcp_status", {}, true);
              if (
                typeof verified !== "object" ||
                verified === null ||
                !("mcpServers" in verified) ||
                !Array.isArray(verified.mcpServers) ||
                !verified.mcpServers.some(
                  (s) => s.name === "t3-code" && s.source === "sdk" && s.status === "connected",
                )
              )
                return yield* new ClaudeCloudGuardError({ reason: "tools_unavailable" });
            }
          });
          yield* ensureMcpConnected;
          connecting = false;
          if (!usable || recoveryPending)
            return yield* new ClaudeCloudGuardError({
              reason: recoveryPending ? "delivery_pending" : "remote_failed",
            });
          const applyReasoning = Effect.gen(function* () {
            yield* control(
              "apply_flag_settings",
              { settings: { effortLevel: selectedEffort, showThinkingSummaries } },
              true,
            );
            const applied = yield* control("get_settings", {}, true).pipe(
              Effect.flatMap(decodeAppliedReasoning),
            );
            if (selectedEffort !== null && applied.applied.effort !== selectedEffort)
              return yield* new ClaudeCloudGuardError({ reason: "reasoning_unavailable" });
          }).pipe(
            Effect.mapError(
              () =>
                new ClaudeCloudGuardError({
                  reason: recoveryPending ? "delivery_pending" : "reasoning_unavailable",
                }),
            ),
          );
          const runtime: ClaudeAgentSdkQuerySession = {
            messages: Stream.fromQueue(queue),
            cloudEventSequence: (message) => sdkSequences.get(message),
            get pendingCloudTurn() {
              return recoveryPaused ? recoveringTurn : undefined;
            },
            cloudEventWasDurable: (message) => {
              const cursor = sdkSequences.get(message);
              return (
                recoveringTurn !== undefined &&
                input.durableCloudSequence !== undefined &&
                cursor !== undefined &&
                BigInt(cursor) <= BigInt(input.durableCloudSequence)
              );
            },
            ...(recoveringTurn === undefined
              ? {}
              : {
                  resumePendingCloudTurn: Effect.gen(function* () {
                    if (closed || !usable || recoveryPending || !recoveryPaused)
                      return yield* new ClaudeCloudGuardError({ reason: "delivery_pending" });
                    yield* Effect.all([guard, checkModel], { concurrency: "unbounded" });
                    lastBillingCheck = DateTime.toEpochMillis(yield* DateTime.now);
                    yield* replayLock.withPermits(1)(
                      Effect.gen(function* () {
                        if (closed || !usable || recoveryPending || !recoveryPaused)
                          return yield* new ClaudeCloudGuardError({ reason: "delivery_pending" });
                        recoveryPaused = false;
                        active = true;
                        replay = [...recoveryBacklog, ...replay];
                        recoveryBacklog.length = 0;
                      }),
                    );
                    yield* Queue.offer(pollWake, undefined);
                  }).pipe(admission.withPermits(1), Effect.mapError(wrap("recover"))),
                }),
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
                yield* applyReasoning;
                yield* ensureMcpConnected;
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
                const promptUuid = message.uuid ?? (yield* crypto.randomUUIDv4());
                forwarded = {
                  ...forwarded,
                  uuid: promptUuid as NonNullable<SDKUserMessage["uuid"]>,
                };
                yield* updateBinding((current) => ({
                  ...current,
                  pendingTurn: { promptUuid, replayAfterSequence: sequence.toString() },
                }));
                if (closed || !usable || recoveryPending)
                  return yield* new ClaudeCloudGuardError({
                    reason: recoveryPending ? "delivery_pending" : "remote_failed",
                  });
                active = true;
                yield* request(`${sessionPath}/events`, {
                  events: [{ payload: { ...forwarded, session_id: binding.cloudId } }],
                }).pipe(
                  // A failed response does not prove the prompt was rejected. Keep reading
                  // this conversation instead of interrupting it or posting the prompt twice.
                  Effect.catchIf(
                    (error) => !isClaudeCloudHttpError(error),
                    () =>
                      Effect.logWarning(
                        "Claude cloud prompt delivery is uncertain; awaiting existing session events.",
                      ),
                  ),
                  Effect.tapError(() =>
                    Effect.gen(function* () {
                      active = false;
                      yield* control("interrupt").pipe(Effect.ignore);
                    }),
                  ),
                );
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
              Effect.gen(function* () {
                if (settings.fastMode || settings.ultracode)
                  return yield* unsupported("applyFlagSettings");
                if (closed || !usable || recoveryPending || active)
                  return yield* new ClaudeCloudGuardError({
                    reason: recoveryPending ? "delivery_pending" : "unsupported_policy",
                  });
                if (settings.effortLevel !== undefined) selectedEffort = settings.effortLevel;
                if (
                  settings.showThinkingSummaries !== undefined &&
                  settings.showThinkingSummaries !== null
                )
                  showThinkingSummaries = settings.showThinkingSummaries;
                yield* guard;
                yield* applyReasoning;
              }).pipe(admission.withPermits(1), Effect.mapError(wrap("applyFlagSettings"))),
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
