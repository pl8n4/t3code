import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  CommandId,
  MessageId,
  ProjectId,
  OrchestratorMcpFailure,
  RuntimeRequestId,
  NodeId,
  RunId,
  ProviderInstanceId,
  ThreadId,
  ScheduledTaskId,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer, Tool, Toolkit } from "effect/ai";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as ThreadHandlers from "../mcp/toolkits/thread/handlers.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import { liveThreadShell, idleThreadProjection } from "../mcp/McpToolAccess.testkit.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { HttpClient, HttpClientResponse } from "effect/http";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as ServerConfig from "../config.ts";
import * as Settings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as Policy from "./ClaudeCloudExecutionPolicy.ts";

const cloudId = ProviderInstanceId.make("cloud");
const codexId = ProviderInstanceId.make("codex");
const root = {
  threadId: ThreadId.make("root"),
  modelSelection: { instanceId: cloudId, model: "claude-opus-5-5" },
};
const child = {
  threadId: ThreadId.make("child"),
  modelSelection: { instanceId: codexId, model: "gpt-6.1-sol" },
};
const localClaude = {
  threadId: ThreadId.make("local"),
  modelSelection: { instanceId: ProviderInstanceId.make("claude"), model: "claude-opus-5-5" },
};

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped();
  const home = `${directory}/claude`;
  const localHome = `${directory}/local-claude`;
  yield* fs.makeDirectory(home);
  yield* fs.makeDirectory(localHome);
  yield* fs.writeFileString(
    `${home}/.claude.json`,
    JSON.stringify({ oauthAccount: { organizationUuid: "org-one" } }),
  );
  yield* fs.writeFileString(
    `${localHome}/.claude.json`,
    JSON.stringify({ oauthAccount: { organizationUuid: "org-one" } }),
  );
  yield* fs.writeFileString(
    `${localHome}/.credentials.json`,
    JSON.stringify({ claudeAiOauth: { accessToken: "test-oauth" } }),
  );
  const configuration = yield* Ref.make<Pick<ServerSettings, "providerInstances">>({
    providerInstances: {
      [cloudId]: {
        driver: "claudeAgent",
        config: { homePath: home, cloudFreeCreditsOnly: true, cloudCodexDelegationEnabled: true },
      },
      [codexId]: { driver: "codex", config: { homePath: `${directory}/codex` } },
      claude: { driver: "claudeAgent", config: { homePath: localHome } },
    },
  } as unknown as Pick<ServerSettings, "providerInstances">);
  const account = yield* Ref.make("chatgpt");
  const hydrated = yield* Ref.make(true);
  const codexHydrated = yield* Ref.make(true);
  const cloudHarness = yield* Ref.make(true);
  const claudeAccount = yield* Ref.make("pro");
  const billing = yield* Ref.make<unknown>({ extra_usage: { is_enabled: false } });
  const billingStatus = yield* Ref.make(200);
  let billingReads = 0;
  const claudeProvider = {
    instanceId: localClaude.modelSelection.instanceId,
    driverKind: "claudeAgent",
    enabled: true,
    continuationIdentity: { continuationKey: `claude:home:${localHome}` },
    snapshot: {
      refresh: Effect.gen(function* () {
        return {
          status: "ready",
          auth: { status: "authenticated", type: yield* Ref.get(claudeAccount) },
          models: [{ slug: "claude-opus-5-5", isCustom: false }],
        } as unknown as ServerProvider;
      }),
    },
  };
  let probes = 0;
  const provider = {
    instanceId: codexId,
    driverKind: "codex",
    enabled: true,
    snapshot: {
      refresh: Effect.gen(function* () {
        probes++;
        return {
          status: "ready",
          auth: { status: "authenticated", type: yield* Ref.get(account) },
          models: [{ slug: "gpt-6.1-sol", isCustom: false }],
        } as unknown as ServerProvider;
      }),
    },
  };
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.gen(function* () {
          billingReads++;
          assert.strictEqual(request.url, "https://api.anthropic.com/api/oauth/usage");
          assert.strictEqual(request.method, "GET");
          return HttpClientResponse.fromWeb(
            request,
            Response.json(yield* Ref.get(billing), { status: yield* Ref.get(billingStatus) }),
          );
        }),
      ),
    ),
    Layer.succeed(ServerConfig.ServerConfig, {
      stateDir: `${directory}/state`,
    } as ServerConfig.ServerConfig["Service"]),
    Layer.mock(Settings.ServerSettingsService)({
      getSettings: Ref.get(configuration).pipe(
        Effect.map((value) => value as unknown as ServerSettings),
      ),
    }),
    Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
      getInstanceForConfig: (id) =>
        Effect.gen(function* () {
          if (!(yield* Ref.get(hydrated)) || (id === codexId && !(yield* Ref.get(codexHydrated))))
            return undefined;
          return id === cloudId
            ? ({
                instanceId: cloudId,
                enabled: true,
                driverKind: "claudeAgent",
                continuationIdentity: {
                  continuationKey: (yield* Ref.get(cloudHarness))
                    ? `claude:home:${home}:cloud-local-v2:${cloudId}`
                    : `claude:home:${home}`,
                },
              } as never)
            : id === localClaude.modelSelection.instanceId
              ? (claudeProvider as never)
              : (provider as never);
        }),
    }),
  );
  const reloadPolicy = Policy.ClaudeCloudExecutionPolicy.pipe(
    Effect.provide(Policy.layer.pipe(Layer.provide(dependencies))),
  );
  const policy = yield* reloadPolicy;
  return {
    policy,
    directory,
    home,
    localHome,
    configuration,
    account,
    hydrated,
    codexHydrated,
    cloudHarness,
    claudeAccount,
    billing,
    billingStatus,
    billingReads: () => billingReads,
    reloadPolicy,
    probes: () => probes,
    fs,
  };
});
const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | import("effect/Crypto").Crypto
    | FileSystem.FileSystem
    | import("effect/Path").Path
    | import("effect/Scope").Scope
  >,
) =>
  effect.pipe(
    Effect.provideService(HostProcess.Environment, {}),
    Effect.provide(NodeServices.layer),
    Effect.scoped,
  );

describe("ClaudeCloudExecutionPolicy", () => {
  it.effect.each([
    { createdBy: "user", creationSource: "web", scheduled: false, allowed: true },
    { createdBy: "user", creationSource: "mobile", scheduled: false, allowed: true },
    { createdBy: "user", creationSource: "web", scheduled: true, allowed: false },
    { createdBy: "user", creationSource: "mobile", scheduled: true, allowed: false },
    { createdBy: "user", creationSource: "mcp", scheduled: false, allowed: false },
    { createdBy: "agent", creationSource: "mcp", scheduled: false, allowed: false },
  ] as const)(
    "dispatches the first local handoff only for a manual composer message: %j",
    (origin) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture;
          yield* f.policy.admit(root);
          const schedule = ScheduledTaskId.make("composer-schedule");
          yield* f.policy.bindSchedule(root, schedule);
          let dispatched = 0;
          const layer = ThreadManagement.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(Policy.ClaudeCloudExecutionPolicy, f.policy),
                Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
                Settings.layerTest({
                  providerInstances: {
                    [localClaude.modelSelection.instanceId]: {
                      driver: "claudeAgent",
                      enabled: true,
                    },
                  },
                }),
                CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
                Layer.mock(Orchestrator.OrchestratorV2)({
                  getThreadRecords: () =>
                    Effect.succeed({
                      thread: {
                        id: root.threadId,
                        projectId: ProjectId.make("project"),
                        modelSelection: root.modelSelection,
                        worktreePath: null,
                      },
                    } as never),
                  dispatch: () =>
                    Effect.sync(() => {
                      dispatched++;
                      return { sequence: 1, storedEvents: [] };
                    }),
                }),
              ),
            ),
          );
          const result = yield* Effect.gen(function* () {
            const threads = yield* ThreadManagement.ThreadManagementService;
            return yield* threads.dispatch({
              type: "message.dispatch",
              threadId: root.threadId,
              commandId: CommandId.make("first-local-switch"),
              messageId: MessageId.make("switch-message"),
              text: "Continue with local Claude",
              attachments: [],
              modelSelection: localClaude.modelSelection,
              dispatchMode: { type: "start_immediately" },
              createdBy: origin.createdBy,
              creationSource: origin.creationSource,
              ...(origin.scheduled ? { scheduledTaskId: schedule } : {}),
            });
          }).pipe(Effect.provide(layer), Effect.result);
          assert.strictEqual(result._tag, origin.allowed ? "Success" : "Failure");
          assert.strictEqual(dispatched, origin.allowed ? 1 : 0);
          const later = yield* f.policy
            .admit({ ...root, modelSelection: localClaude.modelSelection })
            .pipe(Effect.result);
          assert.strictEqual(later._tag, origin.allowed ? "Success" : "Failure");
        }),
      ),
  );
  it.effect("persists a human-selected local Claude handoff and rechecks it after restart", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.policy.admit(root);
        const target = { ...root, modelSelection: localClaude.modelSelection };
        const schedule = ScheduledTaskId.make("existing-schedule");
        yield* f.policy.bindSchedule(root, schedule);
        assert.strictEqual((yield* f.policy.admit(target).pipe(Effect.result))._tag, "Failure");
        // Exhausted promotional credit must not block a separately verified subscription.
        yield* Ref.set(f.billing, {
          extra_usage: { is_enabled: false },
          iguana_necktie: { remaining_dollars: 0 },
        });
        const authorized = yield* f.policy.admit({ ...target, userInitiated: true });
        assert.strictEqual(
          authorized?.localClaudeInstanceId,
          localClaude.modelSelection.instanceId,
        );
        const restarted = yield* f.reloadPolicy;
        yield* restarted.admit(target);
        assert.strictEqual(f.billingReads(), 2);
        yield* restarted.prepareScheduled(schedule, target);
        yield* Ref.set(f.billing, { extra_usage: { is_enabled: true } });
        assert.strictEqual((yield* restarted.admit(target).pipe(Effect.result))._tag, "Failure");
        yield* restarted.admit(root);
      }),
    ),
  );
  it.effect("cannot authorize local Claude through children or scheduled work", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.policy.admit(root);
        assert.strictEqual(
          (yield* f.policy.inherit(root, localClaude).pipe(Effect.result))._tag,
          "Failure",
        );
        yield* f.policy.inherit(root, child);
        assert.strictEqual(
          (yield* f.policy
            .admit({ ...child, modelSelection: localClaude.modelSelection, userInitiated: true })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        const task = ScheduledTaskId.make("local-switch");
        yield* f.policy.bindSchedule(root, task);
        assert.strictEqual(
          (yield* f.policy
            .prepareScheduled(task, { ...root, modelSelection: localClaude.modelSelection })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        assert.strictEqual(f.billingReads(), 0);
      }),
    ),
  );
  it.effect("keeps authorized local Claude descendants on the verified subscription route", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.policy.admit(root);
        const target = { ...root, modelSelection: localClaude.modelSelection, userInitiated: true };
        yield* f.policy.admit(target);
        yield* f.policy.inherit(target, localClaude);
        yield* f.policy.admit(localClaude);
        yield* Ref.set(f.claudeAccount, "apiKey");
        assert.strictEqual(
          (yield* f.policy.admit(localClaude).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );
  it.effect.each([{}, { extra_usage: null }, { extra_usage: { is_enabled: true } }])(
    "refuses unverified or paid local Claude billing: %j",
    (billing) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture;
          yield* f.policy.admit(root);
          yield* Ref.set(f.billing, billing);
          const target = {
            ...root,
            modelSelection: localClaude.modelSelection,
            userInitiated: true,
          };
          assert.strictEqual((yield* f.policy.admit(target).pipe(Effect.result))._tag, "Failure");
          yield* Ref.set(f.billing, { extra_usage: { is_enabled: false } });
          assert.strictEqual(
            (yield* f.policy.admit({ ...target, userInitiated: false }).pipe(Effect.result))._tag,
            "Failure",
          );
        }),
      ),
  );
  it.effect.each([401, 429, 500])("refuses a local billing check returning HTTP %s", (status) =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.policy.admit(root);
        yield* Ref.set(f.billingStatus, status);
        assert.strictEqual(
          (yield* f.policy
            .admit({ ...root, modelSelection: localClaude.modelSelection, userInitiated: true })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );
  it.effect.each(["apiKey", "bedrock", "unknown"])(
    "refuses local Claude authentication %s",
    (auth) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture;
          yield* f.policy.admit(root);
          yield* Ref.set(f.claudeAccount, auth);
          assert.strictEqual(
            (yield* f.policy
              .admit({ ...root, modelSelection: localClaude.modelSelection, userInitiated: true })
              .pipe(Effect.result))._tag,
            "Failure",
          );
          assert.strictEqual(f.billingReads(), 0);
        }),
      ),
  );
  it.effect.each([
    "environment",
    "helper",
    "project",
    "launchArgs",
    "model",
    "account",
    "credentials",
    "fastMode",
  ] as const)("refuses local Claude with unsafe %s", (kind) =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.policy.admit(root);
        const cwd = `${f.directory}/project/nested`;
        yield* f.fs.makeDirectory(cwd, { recursive: true });
        if (kind === "helper")
          yield* f.fs.writeFileString(
            `${f.localHome}/settings.json`,
            JSON.stringify({ apiKeyHelper: "some-command" }),
          );
        if (kind === "project") {
          yield* f.fs.makeDirectory(`${f.directory}/project/.claude`);
          yield* f.fs.writeFileString(
            `${f.directory}/project/.claude/settings.local.json`,
            JSON.stringify({ env: { ANTHROPIC_BASE_URL: "https://example.invalid" } }),
          );
        }
        if (kind === "environment" || kind === "launchArgs")
          yield* Ref.update(f.configuration, (all) => ({
            ...all,
            providerInstances: {
              ...all.providerInstances,
              claude: {
                ...all.providerInstances[localClaude.modelSelection.instanceId]!,
                ...(kind === "environment"
                  ? {
                      environment: [
                        { name: "ANTHROPIC_API_KEY", value: "paid-key", sensitive: true },
                      ],
                    }
                  : { config: { homePath: f.localHome, launchArgs: "--settings paid.json" } }),
              },
            },
          }));
        if (kind === "credentials") yield* f.fs.remove(`${f.localHome}/.credentials.json`);
        if (kind === "account")
          yield* f.fs.writeFileString(
            `${f.localHome}/.claude.json`,
            JSON.stringify({ oauthAccount: { organizationUuid: "other-account" } }),
          );
        const target = {
          ...root,
          cwd,
          modelSelection: {
            ...localClaude.modelSelection,
            ...(kind === "model" ? { model: "custom-paid-model" } : {}),
            ...(kind === "fastMode" ? { options: [{ id: "fastMode", value: true }] } : {}),
          },
          userInitiated: true,
        };
        assert.strictEqual((yield* f.policy.admit(target).pipe(Effect.result))._tag, "Failure");
        assert.strictEqual(f.billingReads(), 0);
      }),
    ),
  );
  it.effect(
    "permits cloud work and requested subscription Codex children, but pins descendants",
    () =>
      run(
        Effect.gen(function* () {
          const { policy, probes } = yield* fixture;
          yield* policy.admit(root);
          yield* policy.inherit(root, child);
          yield* policy.admit(child);
          assert.strictEqual(probes(), 2);
          const paid = yield* policy.inherit(child, localClaude).pipe(Effect.result);
          assert.strictEqual(paid._tag, "Failure");
          const switched = yield* policy
            .admit({ ...child, modelSelection: localClaude.modelSelection })
            .pipe(Effect.result);
          assert.strictEqual(switched._tag, "Failure");
        }),
      ),
  );
  it.effect("refuses API auth at launch and rechecks auth on every later wake", () =>
    run(
      Effect.gen(function* () {
        const { policy, account } = yield* fixture;
        yield* policy.inherit(root, child);
        yield* Ref.set(account, "apiKey");
        const wake = yield* policy.admit(child).pipe(Effect.result);
        assert.strictEqual(wake._tag, "Failure");
        assert.match(wake._tag === "Failure" ? wake.failure.message : "", /ChatGPT subscription/);
      }),
    ),
  );
  it.effect("does not affect unrelated normal threads", () =>
    run(
      Effect.gen(function* () {
        const { policy, probes } = yield* fixture;
        assert.strictEqual(yield* policy.admit(localClaude), undefined);
        yield* policy.inherit(localClaude, {
          ...child,
          modelSelection: localClaude.modelSelection,
        });
        assert.strictEqual(probes(), 0);
      }),
    ),
  );
  it.effect("refuses provider changes and account changes after a guarded task is accepted", () =>
    run(
      Effect.gen(function* () {
        const { policy, fs, home, configuration } = yield* fixture;
        yield* policy.inherit(root, child);
        yield* fs.writeFileString(
          `${home}/.claude.json`,
          JSON.stringify({ oauthAccount: { organizationUuid: "org-two" } }),
        );
        assert.strictEqual((yield* policy.admit(child).pipe(Effect.result))._tag, "Failure");
        yield* fs.writeFileString(
          `${home}/.claude.json`,
          JSON.stringify({ oauthAccount: { organizationUuid: "org-one" } }),
        );
        yield* Ref.update(configuration, (all) => ({
          ...all,
          providerInstances: {
            ...all.providerInstances,
            [cloudId]: {
              ...all.providerInstances[cloudId]!,
              config: { homePath: home, cloudFreeCreditsOnly: false },
            },
          },
        }));
        assert.strictEqual((yield* policy.admit(child).pipe(Effect.result))._tag, "Failure");
      }),
    ),
  );
  it.effect("keeps future scheduled fresh threads guarded after the task target changes", () =>
    run(
      Effect.gen(function* () {
        const { policy } = yield* fixture;
        const taskId = ScheduledTaskId.make("scheduled-job");
        yield* policy.bindSchedule(root, taskId);
        const paid = yield* policy.prepareScheduled(taskId, localClaude).pipe(Effect.result);
        assert.strictEqual(paid._tag, "Failure");
        yield* policy.prepareScheduled(taskId, child);
        assert.strictEqual(
          (yield* policy
            .admit({ ...child, modelSelection: localClaude.modelSelection })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );
  it.effect("prevents PR watching or sending work to an unrelated thread", () =>
    run(
      Effect.gen(function* () {
        const { policy } = yield* fixture;
        yield* policy.assertRelated(root, root);
        assert.strictEqual(
          (yield* policy.assertRelated(root, child).pipe(Effect.result))._tag,
          "Failure",
        );
        yield* policy.inherit(root, child);
        yield* policy.assertRelated(root, child);
        assert.strictEqual(
          (yield* policy.assertRelated(root, localClaude).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );
  it.effect("rejects Codex route config in the home or a workspace ancestor", () =>
    run(
      Effect.gen(function* () {
        const { policy, fs, directory } = yield* fixture;
        yield* fs.makeDirectory(`${directory}/project/.codex`, { recursive: true });
        yield* fs.makeDirectory(`${directory}/project/nested`, { recursive: true });
        yield* fs.writeFileString(
          `${directory}/project/.codex/config.toml`,
          'model_provider = "paid"',
        );
        const result = yield* policy
          .inherit(root, { ...child, cwd: `${directory}/project/nested` })
          .pipe(Effect.result);
        assert.strictEqual(result._tag, "Failure");
      }),
    ),
  );
  it.effect.skipIf(!symlinksSupported)(
    "checks canonical and lexical config ancestors through a symlinked workspace",
    () =>
      run(
        Effect.gen(function* () {
          const { policy, fs, directory } = yield* fixture;
          const repository = `${directory}/real-parent/repository`;
          const nested = `${repository}/nested`;
          const selectedParent = `${directory}/selected-parent`;
          const alias = `${selectedParent}/workspace`;
          yield* fs.makeDirectory(`${repository}/.codex`, { recursive: true });
          yield* fs.makeDirectory(nested);
          yield* fs.makeDirectory(`${selectedParent}/.codex`, { recursive: true });
          yield* fs.symlink(nested, alias);
          yield* policy.inherit(root, { ...child, cwd: alias });
          yield* fs.writeFileString(`${repository}/.codex/config.toml`, 'model_provider = "paid"');
          for (const cwd of [alias, `${alias}/..`]) {
            assert.strictEqual(
              (yield* policy.admit({ ...child, cwd }).pipe(Effect.result))._tag,
              "Failure",
            );
          }
          yield* fs.remove(`${repository}/.codex/config.toml`);
          yield* fs.writeFileString(
            `${selectedParent}/.codex/config.toml`,
            'model_provider = "paid"',
          );
          assert.strictEqual(
            (yield* policy.admit({ ...child, cwd: alias }).pipe(Effect.result))._tag,
            "Failure",
          );
          yield* fs.remove(`${selectedParent}/.codex/config.toml`);
          yield* policy.admit({ ...child, cwd: alias });
          yield* fs.remove(nested, { recursive: true });
          assert.strictEqual(
            (yield* policy.admit({ ...child, cwd: alias }).pipe(Effect.result))._tag,
            "Failure",
          );
        }),
      ),
  );

  it.effect("requires an existing directory when verifying a Codex workspace", () =>
    run(
      Effect.gen(function* () {
        const { policy, fs, directory } = yield* fixture;
        const file = `${directory}/plain-file`;
        yield* fs.writeFileString(file, "not a workspace");
        for (const cwd of [`${directory}/missing`, file]) {
          const rejected = yield* policy.inherit(root, { ...child, cwd }).pipe(Effect.result);
          assert.strictEqual(rejected._tag, "Failure");
          assert.match(
            rejected._tag === "Failure" ? rejected.failure.message : "",
            /workspace directory/,
          );
        }
        yield* policy.inherit(root, { ...child, cwd: directory });
      }),
    ),
  );

  it.effect.each([
    ["Unicode-escaped root key", String.raw`"model_\u0070rovider" = "paid"`],
    [
      "Unicode-escaped provider table",
      String.raw`["model_\u0070roviders".paid]
name = "Paid"`,
    ],
    [
      "quoted profile table",
      String.raw`["pro\u0066iles".paid]
model = "gpt-6.1-sol"`,
    ],
    [
      "nested array-table route",
      String.raw`[[agents]]
"base_\u0075rl" = "https://example.invalid/v1"`,
    ],
    ["built-in OpenAI endpoint override", 'openai_base_url = "https://example.invalid/v1"'],
    ["malformed TOML", 'model = ["gpt-6.1-sol"'],
  ])("rejects %s in Codex configuration", ([, text]) =>
    run(
      Effect.gen(function* () {
        const { policy, fs, directory } = yield* fixture;
        yield* fs.makeDirectory(`${directory}/codex`);
        yield* fs.writeFileString(`${directory}/codex/config.toml`, text!);
        const rejected = yield* policy.inherit(root, child).pipe(Effect.result);
        assert.strictEqual(rejected._tag, "Failure");
        assert.match(
          rejected._tag === "Failure" ? rejected.failure.message : "",
          text === 'model = ["gpt-6.1-sol"'
            ? /Could not parse Codex configuration/
            : /provider or authentication overrides/,
        );
      }),
    ),
  );

  it.effect("allows route terminology inside TOML comments, strings, and project paths", () =>
    run(
      Effect.gen(function* () {
        const { policy, fs, directory } = yield* fixture;
        yield* fs.makeDirectory(`${directory}/codex`);
        yield* fs.writeFileString(
          `${directory}/codex/config.toml`,
          String.raw`# model_provider and profiles below are documentation only
model = "gpt-6.1-sol"
"approval_\u0070olicy" = "never"
developer_instructions = "Explain model_provider, base_url, and #profile terminology."
[projects."/tmp/model_provider"]
trust_level = "trusted"
`,
        );
        yield* policy.inherit(root, child);
      }),
    ),
  );

  it.effect("enforces the tool allowlist on subscription Codex descendants before dispatch", () =>
    run(
      Effect.gen(function* () {
        const { policy } = yield* fixture;
        yield* policy.inherit(root, child);
        let helperInvocations = 0;
        const tool = Tool.make("write_pull_request_guides", {
          parameters: Tool.EmptyParams,
          success: Schema.Boolean,
          failure: OrchestratorMcpFailure,
          failureMode: "return",
          dependencies: [McpInvocationContext.McpInvocationContext],
        });
        const toolkit = Toolkit.make(tool);
        const handlers = McpToolAccess.toLayer(toolkit, {
          write_pull_request_guides: McpToolAccess.reads(() =>
            Effect.sync(() => {
              helperInvocations++;
              return true;
            }),
          ),
        });
        const routes = McpHttpServer.toolkitRegistration(toolkit, handlers).pipe(
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(Layer.succeed(Policy.ClaudeCloudExecutionPolicy, policy)),
        );
        const client = McpSchema.McpServerClient.of({
          clientId: 1,
          protocolVersion: "2025-06-18",
          clientCapabilities: {},
          clientInfo: { name: "guard-test", version: "1" },
          initializePayload: {
            protocolVersion: "2025-06-18",
            capabilities: {},
            clientInfo: { name: "guard-test", version: "1" },
          },
          getClient: Effect.die("unused"),
        });
        const result = yield* McpServer.McpServer.pipe(
          Effect.flatMap((server) =>
            server.callTool({ name: "write_pull_request_guides", arguments: {} }),
          ),
          Effect.provide(routes),
          Effect.provideService(McpSchema.McpServerClient, client),
          Effect.provideService(McpInvocationContext.McpInvocationContext, {
            environmentId: EnvironmentId.make("test"),
            requestNamespace: "guard-test",
            thread: {
              threadId: child.threadId,
              providerSessionId: "test-session",
              providerInstanceId: codexId,
            },
            client: undefined,
            capabilities: new Set(["orchestration" as const]),
            issuedAt: 0,
          }),
        );
        assert.isTrue(result.isError);
        assert.strictEqual(helperInvocations, 0);
      }),
    ),
  );
  it.effect("blocks unknown future tools and bounds nested delegation", () =>
    run(
      Effect.gen(function* () {
        const { policy } = yield* fixture;
        yield* policy.admit(root);
        yield* policy.guardTool(root.threadId, "delegate_task");
        assert.strictEqual(
          (yield* policy.guardTool(root.threadId, "future_paid_helper").pipe(Effect.result))._tag,
          "Failure",
        );
        let source = root;
        for (let depth = 1; depth <= 3; depth++) {
          const next = { ...root, threadId: ThreadId.make(`nested-${depth}`) };
          yield* policy.inherit(source, next);
          source = next;
        }
        assert.strictEqual(
          (yield* policy
            .inherit(source, { ...root, threadId: ThreadId.make("too-deep") })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );

  it.effect(
    "checks the environment-selected Codex home and respects explicit-home precedence",
    () =>
      run(
        Effect.gen(function* () {
          const { policy, fs, directory, configuration } = yield* fixture;
          const alternate = `${directory}/alternate-codex`;
          yield* fs.makeDirectory(alternate);
          yield* fs.writeFileString(`${alternate}/config.toml`, 'model_provider = "paid"');
          yield* Ref.update(configuration, (all) => ({
            ...all,
            providerInstances: {
              ...all.providerInstances,
              [codexId]: {
                ...all.providerInstances[codexId]!,
                config: { homePath: "" },
                environment: [{ name: "CODEX_HOME", value: alternate, sensitive: false }],
              },
            },
          }));
          const envRoute = yield* policy.inherit(root, child).pipe(Effect.result);
          assert.strictEqual(envRoute._tag, "Failure");
          yield* Ref.update(configuration, (all) => ({
            ...all,
            providerInstances: {
              ...all.providerInstances,
              [codexId]: {
                ...all.providerInstances[codexId]!,
                config: { homePath: `${directory}/clean-codex` },
              },
            },
          }));
          yield* policy.inherit(root, child);
        }),
      ),
  );
  it.effect("refuses a provider HOME that hides API routing from subscription probes", () =>
    run(
      Effect.gen(function* () {
        const { policy, fs, directory, configuration } = yield* fixture;
        const alternateHome = `${directory}/alternate-user-home`;
        yield* fs.makeDirectory(`${alternateHome}/.codex`, { recursive: true });
        yield* fs.writeFileString(
          `${alternateHome}/.codex/config.toml`,
          'model_provider = "paid"\n[model_providers.paid]\nbase_url = "https://example.invalid/v1"',
        );
        yield* Ref.update(configuration, (all) => ({
          ...all,
          providerInstances: {
            ...all.providerInstances,
            [codexId]: {
              ...all.providerInstances[codexId]!,
              config: { homePath: "" },
              environment: [{ name: "HOME", value: alternateHome, sensitive: false }],
            },
          },
        }));
        const admitted = yield* policy.inherit(root, child).pipe(Effect.result);
        assert.strictEqual(admitted._tag, "Failure");
        assert.match(
          admitted._tag === "Failure" ? admitted.failure.message : "",
          /home-directory overrides/,
        );
        yield* Ref.update(configuration, (all) => ({
          ...all,
          providerInstances: {
            ...all.providerInstances,
            [codexId]: {
              ...all.providerInstances[codexId]!,
              environment: [{ name: "HOME", value: "", sensitive: false }],
            },
          },
        }));
        assert.strictEqual(
          (yield* policy.inherit(root, child).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );

  it.effect("permits inherited HOME and an unchanged provider HOME", () =>
    run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const inheritedHome = yield* fs.makeTempDirectoryScoped();
        const { policy, configuration } = yield* fixture.pipe(
          Effect.provideService(HostProcess.Environment, { HOME: inheritedHome }),
        );
        yield* policy.inherit(root, child);
        yield* Ref.update(configuration, (all) => ({
          ...all,
          providerInstances: {
            ...all.providerInstances,
            [codexId]: {
              ...all.providerInstances[codexId]!,
              environment: [{ name: "HOME", value: inheritedHome, sensitive: false }],
            },
          },
        }));
        yield* policy.admit(child);
      }),
    ),
  );

  it.effect("refuses stale cloud or Codex runtime configurations during settings hydration", () =>
    run(
      Effect.gen(function* () {
        const { policy, hydrated, codexHydrated, cloudHarness } = yield* fixture;
        yield* Ref.set(cloudHarness, false);
        assert.isTrue(yield* policy.isGuarded(root));
        assert.strictEqual((yield* policy.admit(root).pipe(Effect.result))._tag, "Failure");
        yield* Ref.set(cloudHarness, true);
        yield* policy.inherit(root, child);
        yield* Ref.set(codexHydrated, false);
        assert.strictEqual((yield* policy.admit(child).pipe(Effect.result))._tag, "Failure");
        yield* Ref.set(codexHydrated, true);
        yield* Ref.set(hydrated, false);
        assert.strictEqual((yield* policy.admit(root).pipe(Effect.result))._tag, "Failure");
        yield* Ref.set(hydrated, true);
        yield* policy.admit(child);
      }),
    ),
  );

  it.effect("answers guarded child questions and rejects foreign thread wakeups", () =>
    run(
      Effect.gen(function* () {
        const { policy } = yield* fixture;
        yield* policy.inherit(root, child);
        const requestId = RuntimeRequestId.make("question");
        const runId = RunId.make("question-run");
        const nodeId = NodeId.make("question-node");
        let dispatched = 0;
        const shell = (id: typeof root.threadId) => ({
          ...liveThreadShell(id),
          providerInstanceId: id === root.threadId ? cloudId : codexId,
          modelSelection:
            id === root.threadId
              ? root.modelSelection
              : id === child.threadId
                ? child.modelSelection
                : localClaude.modelSelection,
        });
        const threads = Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: (id) => Effect.succeed(shell(id)),
          getProjectThreadRecords: ({ threadId }) =>
            Effect.succeed({
              ...idleThreadProjection(shell(threadId)),
              runs: [{ id: runId, modelSelection: shell(threadId).modelSelection }],
              nodes: [{ id: nodeId, runId }],
              runtimeRequests: [{ id: requestId, kind: "user_input", status: "pending", nodeId }],
              turnItems: [{ type: "user_input_request", requestId, runId, questions: [] }],
            } as never),
          dispatch: () =>
            Effect.sync(() => {
              dispatched++;
              return { sequence: dispatched, storedEvents: [] };
            }),
        });
        const routes = McpHttpServer.toolkitRegistration(ThreadToolkit, ThreadHandlers.layer).pipe(
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(
            Layer.mergeAll(
              threads,
              Layer.mock(ThreadSearch.ThreadSearch)({}),
              Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
            ),
          ),
          Layer.provide(Layer.succeed(Policy.ClaudeCloudExecutionPolicy, policy)),
        );
        const client = McpSchema.McpServerClient.of({
          clientId: 1,
          protocolVersion: "2025-06-18",
          clientCapabilities: {},
          clientInfo: { name: "guard-test", version: "1" },
          initializePayload: {
            protocolVersion: "2025-06-18",
            capabilities: {},
            clientInfo: { name: "guard-test", version: "1" },
          },
          getClient: Effect.die("unused"),
        });
        yield* Effect.gen(function* () {
          const server = yield* McpServer.McpServer;
          const childResult = yield* server.callTool({
            name: "t3_pending_request_respond",
            arguments: { threadId: child.threadId, requestId, answers: {} },
          });
          assert.isNotTrue(childResult.isError);
          assert.strictEqual(dispatched, 1);
          const foreignResult = yield* server.callTool({
            name: "t3_pending_request_respond",
            arguments: { threadId: localClaude.threadId, requestId, answers: {} },
          });
          assert.isTrue(foreignResult.isError);
          assert.strictEqual(dispatched, 1);
        }).pipe(
          Effect.provide(routes),
          Effect.provideService(McpSchema.McpServerClient, client),
          Effect.provideService(McpInvocationContext.McpInvocationContext, {
            environmentId: EnvironmentId.make("test"),
            requestNamespace: "guard-question",
            thread: {
              threadId: root.threadId,
              providerSessionId: "test-session",
              providerInstanceId: cloudId,
            },
            client: undefined,
            capabilities: new Set(["orchestration" as const]),
            issuedAt: 0,
          }),
        );
      }),
    ),
  );
});
