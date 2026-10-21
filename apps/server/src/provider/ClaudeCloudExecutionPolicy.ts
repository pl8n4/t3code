import {
  ClaudeSettings,
  CodexSettings,
  type ModelSelection,
  ProviderInstanceId,
  type ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import { resolveClaudeHomePath } from "./Drivers/ClaudeHome.ts";
import { resolveCodexHomeLayout } from "./Drivers/CodexHomeLayout.ts";
import { parse as parseToml } from "smol-toml";
import { isClaudeCloudToolAllowed } from "./ClaudeCloudAllowedTools.ts";
import { verifyClaudeCloudModel } from "./ClaudeCloudPolicy.ts";

const Binding = Schema.Struct({
  rootThreadId: ThreadId,
  depth: Schema.Int,
  cloudInstanceId: ProviderInstanceId,
  organizationId: Schema.String,
  localClaudeInstanceId: Schema.optional(ProviderInstanceId),
});
export type Binding = typeof Binding.Type;
const BindingJson = Schema.fromJsonString(Binding);
const encodeBinding = Schema.encodeSync(BindingJson);
const Account = Schema.Struct({ oauthAccount: Schema.Struct({ organizationUuid: Schema.String }) });
const decodeBinding = Schema.decodeUnknownEffect(BindingJson);
const decodeAccount = Schema.decodeUnknownEffect(Schema.fromJsonString(Account));
const decodeClaudeSettings = Schema.decodeUnknownEffect(ClaudeSettings);
const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);

export class ClaudeCloudExecutionPolicyError extends Schema.TaggedError<ClaudeCloudExecutionPolicyError>()(
  "ClaudeCloudExecutionPolicyError",
  { detail: Schema.String },
) {
  override get message() {
    return this.detail;
  }
}

const isPolicyError = Schema.is(ClaudeCloudExecutionPolicyError);

interface ThreadTarget {
  readonly threadId: ThreadId;
  readonly modelSelection: ModelSelection;
  readonly cwd?: string;
  readonly userInitiated?: boolean;
}
export class ClaudeCloudExecutionPolicy extends Context.Service<
  ClaudeCloudExecutionPolicy,
  {
    readonly isGuarded: (target: {
      readonly threadId: ThreadId;
      readonly modelSelection?: ModelSelection;
    }) => Effect.Effect<boolean, ClaudeCloudExecutionPolicyError>;
    readonly guardTool: (
      threadId: ThreadId,
      name: string,
    ) => Effect.Effect<void, ClaudeCloudExecutionPolicyError>;
    readonly admit: (
      target: ThreadTarget,
    ) => Effect.Effect<Binding | undefined, ClaudeCloudExecutionPolicyError>;
    readonly inherit: (
      source: ThreadTarget,
      target: ThreadTarget,
    ) => Effect.Effect<void, ClaudeCloudExecutionPolicyError>;
    readonly assertRelated: (
      source: ThreadTarget,
      target: ThreadTarget,
    ) => Effect.Effect<void, ClaudeCloudExecutionPolicyError>;
    readonly assertScheduleRelated: (
      source: ThreadTarget,
      taskId: ScheduledTaskId,
    ) => Effect.Effect<void, ClaudeCloudExecutionPolicyError>;
    readonly bindSchedule: (
      source: ThreadTarget,
      taskId: ScheduledTaskId,
    ) => Effect.Effect<void, ClaudeCloudExecutionPolicyError>;
    readonly prepareScheduled: (
      taskId: ScheduledTaskId,
      target: ThreadTarget,
    ) => Effect.Effect<void, ClaudeCloudExecutionPolicyError>;
  }
>()("t3/provider/ClaudeCloudExecutionPolicy") {}

const unsafeCodexEnvironment =
  /^(?:OPENAI_|AZURE_OPENAI_|CODEX_(?:API|BASE|PROVIDER)|T3CODE_CODEX_LAUNCH_ARGS|ACCESS_TOKEN$)/;
// Reject all route/profile declarations, including quoted TOML keys. Ambiguous
// configuration must never silently turn a subscription-only child into API use.
const unsafeCodexConfigKeys = new Set([
  "model_provider",
  "model_providers",
  "profile",
  "profiles",
  "base_url",
  "openai_base_url",
  "env_key",
  "api_key",
  "experimental_bearer_token",
  "requires_openai_auth",
  "wire_api",
  "chatgpt_base_url",
  "auth_mode",
  "forced_login_method",
]);

const hasUnsafeCodexConfig = (value: unknown): boolean => {
  if (Array.isArray(value)) return value.some(hasUnsafeCodexConfig);
  if (typeof value !== "object" || value === null) return false;
  return Object.entries(value).some(
    ([key, nested]) => unsafeCodexConfigKeys.has(key.toLowerCase()) || hasUnsafeCodexConfig(nested),
  );
};

const unsafeClaudeEnvironment =
  /^(?:ANTHROPIC_|CLAUDE_CODE_(?:USE_|OAUTH_TOKEN|API|BASE|SUBAGENT_MODEL))/i;
const decodeClaudeCredentials = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ claudeAiOauth: Schema.Struct({ accessToken: Schema.NonEmptyString }) }),
  ),
);
const decodeClaudeBilling = Schema.decodeUnknownEffect(
  Schema.Struct({ extra_usage: Schema.Struct({ is_enabled: Schema.Literal(false) }) }),
);
const decodeJsonObject = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const hasUnsafeClaudeSettings = (value: Record<string, unknown>): boolean =>
  value.apiKeyHelper !== undefined ||
  (typeof value.env === "object" &&
    value.env !== null &&
    Object.entries(value.env).some(
      ([key, entry]) => unsafeClaudeEnvironment.test(key) && Boolean(entry),
    ));

export const layer = Layer.effect(
  ClaudeCloudExecutionPolicy,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const crypto = yield* Crypto.Crypto;
    const path = yield* Path.Path;
    const server = yield* ServerConfig.ServerConfig;
    const settings = yield* ServerSettings.ServerSettingsService;
    const instances = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
    const baseEnv = yield* HostProcess.Environment;
    const http = yield* HttpClient.HttpClient;
    const lock = yield* Semaphore.make(1);
    const directory = path.join(server.stateDir, "claude-cloud-execution-policies");
    const fail = (detail: string) => new ClaudeCloudExecutionPolicyError({ detail });
    const storage = (kind: "thread" | "schedule", id: string) =>
      crypto
        .digest("SHA-256", new TextEncoder().encode(id))
        .pipe(
          Effect.map((bytes) =>
            path.join(
              directory,
              `${kind}-${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}.json`,
            ),
          ),
        );
    const read = Effect.fnUntraced(function* (kind: "thread" | "schedule", id: string) {
      const filename = yield* storage(kind, id);
      if (!(yield* fs.exists(filename))) return undefined;
      return yield* fs.readFileString(filename).pipe(Effect.flatMap(decodeBinding));
    });
    const write = Effect.fnUntraced(function* (
      kind: "thread" | "schedule",
      id: string,
      binding: Binding,
      update = false,
    ) {
      const previous = yield* read(kind, id);
      if (previous !== undefined) {
        if (
          previous.rootThreadId !== binding.rootThreadId ||
          previous.cloudInstanceId !== binding.cloudInstanceId ||
          previous.organizationId !== binding.organizationId
        )
          return yield* fail(
            "This workflow belongs to a different promotional-credit cloud account.",
          );
        if (!update) return;
      }
      yield* fs.makeDirectory(directory, { recursive: true });
      const filename = yield* storage(kind, id);
      // The process-wide lock owns this temporary path. Rename commits the guard
      // before any child dispatch or future scheduler wake can start a provider.
      yield* fs.writeFileString(`${filename}.tmp`, encodeBinding(binding));
      yield* fs.rename(`${filename}.tmp`, filename);
    });
    const loadCloud = Effect.fnUntraced(function* (instanceId: ProviderInstanceId) {
      const all = yield* settings.getSettings;
      const entry = all.providerInstances[instanceId];
      if (entry?.driver !== "claudeAgent" || entry.enabled === false) return undefined;
      const config = yield* decodeClaudeSettings(entry.config ?? {});
      if (!config.enabled || !config.cloudFreeCreditsOnly) return undefined;
      const environment = yield* mergeProviderInstanceEnvironment(entry.environment, baseEnv);
      const home = yield* resolveClaudeHomePath(config, environment);
      const instance = yield* instances.getInstanceForConfig(instanceId, entry);
      if (
        instance === undefined ||
        !instance.enabled ||
        instance.driverKind !== "claudeAgent" ||
        instance.continuationIdentity.continuationKey !==
          `claude:home:${home}:cloud-local-v2:${instanceId}`
      )
        return yield* fail(
          "The live provider has not loaded the verified cloud configuration yet. Retry once provider setup finishes.",
        );
      const accountPath =
        config.homePath.trim() || environment.CLAUDE_CONFIG_DIR?.trim()
          ? path.join(home, ".claude.json")
          : path.join(home, "..", ".claude.json");
      const account = yield* fs.readFileString(accountPath).pipe(Effect.flatMap(decodeAccount));
      return { config, organizationId: account.oauthAccount.organizationUuid };
    });
    const assertCodex = Effect.fnUntraced(function* (selection: ModelSelection, cwd?: string) {
      const all = yield* settings.getSettings;
      const entry = all.providerInstances[selection.instanceId];
      if (entry?.driver !== "codex" || entry.enabled === false)
        return yield* fail(
          "Guarded automation permits the original cloud provider or subscription-authenticated Codex only.",
        );
      const config = yield* decodeCodexSettings(entry.config ?? {});
      const environment = yield* mergeProviderInstanceEnvironment(entry.environment, baseEnv);
      // An unmanaged app-server derives its default Codex home from its child
      // environment, while home layout/auth probes use the server's home. Keep
      // those aligned; explicit Codex homes are scanned below instead.
      if (
        entry.environment?.some((variable) => {
          if (!/^(?:HOME|USERPROFILE|HOMEDRIVE|HOMEPATH)$/i.test(variable.name)) return false;
          const inherited = Object.entries(baseEnv).find(
            ([name]) => name.toLowerCase() === variable.name.toLowerCase(),
          )?.[1];
          return variable.value !== inherited;
        })
      )
        return yield* fail(
          "Subscription-only Codex delegation refuses per-provider home-directory overrides. Set the Codex home path or CODEX_HOME instead.",
        );
      if (
        !config.enabled ||
        config.launchArgs.trim() ||
        Object.entries(environment).some(
          ([key, value]) => value && unsafeCodexEnvironment.test(key),
        )
      )
        return yield* fail(
          "Subscription-only Codex delegation refuses API credentials, endpoint overrides, and custom launch arguments.",
        );
      const instance = yield* instances.getInstanceForConfig(selection.instanceId, entry);
      if (instance === undefined || !instance.enabled || instance.driverKind !== "codex")
        return yield* fail(
          "The Codex provider is unavailable or its configuration is still loading.",
        );
      const snapshot = yield* instance.snapshot.refresh;
      const currentEntry = (yield* settings.getSettings).providerInstances[selection.instanceId];
      if (
        !Equal.equals(currentEntry, entry) ||
        (yield* instances.getInstanceForConfig(selection.instanceId, entry)) !== instance
      )
        return yield* fail(
          "The Codex configuration changed during subscription verification. Retry after provider setup finishes.",
        );
      if (
        snapshot.auth.status !== "authenticated" ||
        snapshot.auth.type !== "chatgpt" ||
        snapshot.status !== "ready"
      )
        return yield* fail("Codex delegation requires a verified ChatGPT subscription sign-in.");
      const model = snapshot.models.find(
        (model) => model.slug === selection.model || model.aliases?.includes(selection.model),
      );
      if (model === undefined || model.isCustom || model.subProvider !== undefined)
        return yield* fail("Codex delegation requires a built-in subscription model.");
      if (config.setupMode !== "managed") {
        const layout = yield* resolveCodexHomeLayout({
          ...config,
          homePath:
            config.homePath.trim() || config.shadowHomePath.trim()
              ? config.homePath
              : (environment.CODEX_HOME?.trim() ?? ""),
        });
        const homes = new Set(
          [layout.sharedHomePath, layout.effectiveHomePath].filter(
            (value): value is string => value !== undefined,
          ),
        );
        const files = new Set([...homes].map((home) => path.join(home, "config.toml")));
        // Native cwd resolution follows symlinks before processing `..`. Scan
        // both canonical and lexical ancestors so neither config chain escapes
        // verification when a project was selected through an alias.
        if (cwd !== undefined) {
          const canonical = yield* fs
            .realPath(cwd)
            .pipe(Effect.mapError(() => fail("Could not verify the Codex workspace directory.")));
          const workspace = yield* fs
            .stat(canonical)
            .pipe(Effect.mapError(() => fail("Could not verify the Codex workspace directory.")));
          const selected = yield* fs
            .stat(cwd)
            .pipe(Effect.mapError(() => fail("Could not verify the Codex workspace directory.")));
          if (workspace.type !== "Directory" || selected.type !== "Directory")
            return yield* fail("Codex delegation requires an existing workspace directory.");
          // Node realPath may normalize `..` before following a symlink, while
          // native cwd/stat follow it afterward. Reject a mismatched identity.
          if (
            workspace.dev !== selected.dev ||
            Option.isNone(workspace.ino) ||
            Option.isNone(selected.ino) ||
            workspace.ino.value === 0n ||
            selected.ino.value === 0n ||
            workspace.ino.value !== selected.ino.value
          )
            return yield* fail(
              "Could not verify the canonical Codex workspace directory. Select its direct path.",
            );
          for (const start of new Set([path.resolve(cwd), canonical])) {
            let current = start;
            for (;;) {
              files.add(path.join(current, ".codex", "config.toml"));
              const parent = path.dirname(current);
              if (parent === current) break;
              current = parent;
            }
          }
        }
        for (const filename of files) {
          if (!(yield* fs.exists(filename))) continue;
          const text = yield* fs.readFileString(filename);
          const unsafe = yield* Effect.try({
            try: () => hasUnsafeCodexConfig(parseToml(text)),
            catch: () => fail("Could not parse Codex configuration for subscription verification."),
          });
          if (unsafe)
            return yield* fail(
              "Codex configuration contains provider or authentication overrides. Use a clean subscription-only Codex instance for this workflow.",
            );
        }
      }
    });
    const assertLocalClaude = Effect.fnUntraced(function* (binding: Binding, target: ThreadTarget) {
      const all = yield* settings.getSettings;
      const entry = all.providerInstances[target.modelSelection.instanceId];
      if (entry?.driver !== "claudeAgent" || entry.enabled === false)
        return yield* fail("Select an enabled local Claude subscription instance.");
      const config = yield* decodeClaudeSettings(entry.config ?? {});
      const environment = yield* mergeProviderInstanceEnvironment(entry.environment, baseEnv);
      if (
        !config.enabled ||
        config.cloudFreeCreditsOnly ||
        config.launchArgs.trim() ||
        target.modelSelection.options?.some(
          (option) => option.id === "fastMode" && option.value === true,
        ) ||
        Object.entries(environment).some(
          ([key, value]) => value && unsafeClaudeEnvironment.test(key),
        ) ||
        entry.environment?.some(
          (variable) =>
            /^(?:HOME|USERPROFILE|HOMEDRIVE|HOMEPATH)$/i.test(variable.name) &&
            variable.value !== baseEnv[variable.name],
        )
      )
        return yield* fail(
          "Local Claude continuation refuses API routing and custom launch arguments.",
        );
      const home = yield* resolveClaudeHomePath(config, environment);
      const instance = yield* instances.getInstanceForConfig(
        target.modelSelection.instanceId,
        entry,
      );
      if (
        instance === undefined ||
        !instance.enabled ||
        instance.driverKind !== "claudeAgent" ||
        instance.continuationIdentity.continuationKey !== `claude:home:${home}`
      )
        return yield* fail("The local Claude configuration is still loading.");
      const snapshot = yield* instance.snapshot.refresh;
      if (
        !Equal.equals(
          (yield* settings.getSettings).providerInstances[target.modelSelection.instanceId],
          entry,
        ) ||
        (yield* instances.getInstanceForConfig(target.modelSelection.instanceId, entry)) !==
          instance
      )
        return yield* fail(
          "The local Claude configuration changed during subscription verification. Retry after setup finishes.",
        );
      const auth = snapshot.auth.type?.toLowerCase().replace(/[\s_-]+/g, "");
      if (
        snapshot.status !== "ready" ||
        snapshot.auth.status !== "authenticated" ||
        !auth ||
        ![
          "pro",
          "max",
          "max5x",
          "max20x",
          "claudeprosubscription",
          "claudemaxsubscription",
          "claudemax5xsubscription",
          "claudemax20xsubscription",
        ].includes(auth)
      )
        return yield* fail(
          "Local Claude continuation requires a verified Claude Pro or Max subscription sign-in.",
        );
      const model = snapshot.models.find(
        (m) =>
          m.slug === target.modelSelection.model ||
          m.aliases?.includes(target.modelSelection.model),
      );
      if (model === undefined || model.isCustom || model.subProvider !== undefined)
        return yield* fail("Local Claude continuation requires a built-in subscription model.");
      const accountPath =
        config.homePath.trim() || environment.CLAUDE_CONFIG_DIR?.trim()
          ? path.join(home, ".claude.json")
          : path.join(home, "..", ".claude.json");
      const account = yield* fs.readFileString(accountPath).pipe(Effect.flatMap(decodeAccount));
      if (account.oauthAccount.organizationUuid !== binding.organizationId)
        return yield* fail(
          "Select local Claude signed in to the same account as this cloud workflow.",
        );
      const files = new Set([
        path.join(home, "settings.json"),
        path.join(home, "settings.local.json"),
        "/etc/claude-code/managed-settings.json",
      ]);
      if (target.cwd !== undefined) {
        const canonical = yield* fs.realPath(target.cwd);
        const actual = yield* fs.stat(canonical);
        const selected = yield* fs.stat(target.cwd);
        if (
          actual.type !== "Directory" ||
          selected.type !== "Directory" ||
          actual.dev !== selected.dev ||
          Option.isNone(actual.ino) ||
          Option.isNone(selected.ino) ||
          actual.ino.value === 0n ||
          actual.ino.value !== selected.ino.value
        )
          return yield* fail(
            "Select the existing, direct workspace path for local Claude continuation.",
          );
        for (const start of new Set([canonical, path.resolve(target.cwd)])) {
          let current = start;
          for (;;) {
            files.add(path.join(current, ".claude", "settings.json"));
            files.add(path.join(current, ".claude", "settings.local.json"));
            const parent = path.dirname(current);
            if (parent === current) break;
            current = parent;
          }
        }
      }
      for (const file of files) {
        if (!(yield* fs.exists(file))) continue;
        const value = yield* fs.readFileString(file).pipe(Effect.flatMap(decodeJsonObject));
        if (hasUnsafeClaudeSettings(value))
          return yield* fail(
            "Claude settings contain API routing or an API key helper. Use a clean subscription instance.",
          );
      }
      // Read-only OAuth billing verification works after promotional credit is exhausted.
      const credentials = yield* fs.readFileString(path.join(home, ".credentials.json")).pipe(
        Effect.flatMap(decodeClaudeCredentials),
        Effect.mapError(() =>
          fail("Sign in to local Claude again; subscription credentials are unavailable."),
        ),
      );
      yield* http
        .execute(
          HttpClientRequest.get("https://api.anthropic.com/api/oauth/usage").pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${credentials.claudeAiOauth.accessToken}`,
              "anthropic-beta": "oauth-2025-04-20",
            }),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap((response) => response.json),
          Effect.flatMap(decodeClaudeBilling),
          Effect.timeout("10 seconds"),
          Effect.mapError(() =>
            fail(
              "Could not verify that paid extra usage is disabled for local Claude. No model was started.",
            ),
          ),
        );
    });
    const validate = Effect.fnUntraced(function* (
      binding: Binding,
      target: ThreadTarget,
      manualLocalInstance?: ProviderInstanceId,
    ) {
      const cloud = yield* loadCloud(binding.cloudInstanceId);
      if (cloud === undefined || cloud.organizationId !== binding.organizationId)
        return yield* fail(
          "The original promotional-credit cloud account changed or was disabled. This workflow cannot fall back to another provider.",
        );
      if (target.modelSelection.instanceId === binding.cloudInstanceId) {
        yield* Effect.try({
          try: () => verifyClaudeCloudModel(target.modelSelection.model),
          catch: () => fail("This model is not approved for promotional-credit cloud sessions."),
        });
        return;
      }
      const permissions = yield* read("thread", binding.rootThreadId);
      if (
        (manualLocalInstance ?? permissions?.localClaudeInstanceId) ===
        target.modelSelection.instanceId
      )
        return yield* assertLocalClaude(binding, target);
      if (!cloud.config.cloudCodexDelegationEnabled)
        return yield* fail(
          "Codex delegation has not been enabled for this promotional-credit cloud instance.",
        );
      yield* assertCodex(target.modelSelection, target.cwd);
    });
    const admit = Effect.fnUntraced(function* (target: ThreadTarget) {
      let binding = yield* read("thread", target.threadId);
      if (binding === undefined) {
        const cloud = yield* loadCloud(target.modelSelection.instanceId);
        if (cloud === undefined) return undefined;
        binding = {
          rootThreadId: target.threadId,
          depth: 0,
          cloudInstanceId: target.modelSelection.instanceId,
          organizationId: cloud.organizationId,
        };
        yield* write("thread", target.threadId, binding);
      }
      const selected = (yield* settings.getSettings).providerInstances[
        target.modelSelection.instanceId
      ];
      if (
        target.userInitiated === true &&
        target.threadId === binding.rootThreadId &&
        binding.depth === 0 &&
        selected?.driver === "claudeAgent" &&
        target.modelSelection.instanceId !== binding.cloudInstanceId
      ) {
        const authorized = { ...binding, localClaudeInstanceId: target.modelSelection.instanceId };
        yield* validate(binding, target, target.modelSelection.instanceId);
        yield* write("thread", target.threadId, authorized, true);
        return authorized;
      }
      yield* validate(binding, target);
      return binding;
    });
    const wrap = <A, E>(effect: Effect.Effect<A, E, Path.Path>) =>
      effect.pipe(
        Effect.provideService(Path.Path, path),
        Effect.mapError((cause) =>
          isPolicyError(cause)
            ? cause
            : fail(
                "Could not verify the guarded workflow's execution policy. No provider was started.",
              ),
        ),
      );
    return ClaudeCloudExecutionPolicy.of({
      isGuarded: (target) =>
        wrap(
          Effect.gen(function* () {
            if ((yield* read("thread", target.threadId)) !== undefined) return true;
            if (target.modelSelection === undefined) return false;
            const entry = (yield* settings.getSettings).providerInstances[
              target.modelSelection.instanceId
            ];
            if (entry?.driver !== "claudeAgent") return false;
            return (yield* decodeClaudeSettings(entry.config ?? {})).cloudFreeCreditsOnly;
          }),
        ),
      guardTool: (threadId, name) =>
        wrap(
          Effect.gen(function* () {
            const binding = yield* read("thread", threadId);
            if (binding !== undefined && !isClaudeCloudToolAllowed(name))
              return yield* fail(
                "This tool is disabled in promotional-credit workflows because it can change billing routes or start unguarded model work.",
              );
          }),
        ),
      admit: (target) => lock.withPermit(wrap(admit(target))),
      inherit: (source, target) =>
        lock.withPermit(
          wrap(
            Effect.gen(function* () {
              const binding = yield* admit(source);
              if (binding === undefined) return;
              yield* validate(binding, target);
              const previous = yield* read("thread", target.threadId);
              if (previous === undefined && binding.depth >= 3)
                return yield* fail(
                  "Guarded delegation is limited to three child levels. Return the result to the parent instead of launching another child.",
                );
              yield* write(
                "thread",
                target.threadId,
                previous ?? { ...binding, depth: binding.depth + 1 },
              );
            }),
          ),
        ),
      assertRelated: (source, target) =>
        lock.withPermit(
          wrap(
            Effect.gen(function* () {
              const binding = yield* admit(source);
              if (binding === undefined) return;
              const other = yield* read("thread", target.threadId);
              if (
                other === undefined ||
                other.rootThreadId !== binding.rootThreadId ||
                other.cloudInstanceId !== binding.cloudInstanceId ||
                other.organizationId !== binding.organizationId
              )
                return yield* fail(
                  "Guarded automation cannot wake an unrelated thread. Use this thread or one of its guarded children.",
                );
              yield* validate(binding, target);
            }),
          ),
        ),
      assertScheduleRelated: (source, taskId) =>
        lock.withPermit(
          wrap(
            Effect.gen(function* () {
              const binding = yield* admit(source);
              if (binding === undefined) return;
              const scheduled = yield* read("schedule", taskId);
              if (
                scheduled === undefined ||
                scheduled.rootThreadId !== binding.rootThreadId ||
                scheduled.cloudInstanceId !== binding.cloudInstanceId ||
                scheduled.organizationId !== binding.organizationId
              )
                return yield* fail(
                  "Guarded automation cannot run or modify an unrelated scheduled task.",
                );
            }),
          ),
        ),
      bindSchedule: (source, taskId) =>
        lock.withPermit(
          wrap(
            Effect.gen(function* () {
              const binding = yield* admit(source);
              if (binding !== undefined) yield* write("schedule", taskId, binding);
            }),
          ),
        ),
      prepareScheduled: (taskId, target) =>
        lock.withPermit(
          wrap(
            Effect.gen(function* () {
              const binding = yield* read("schedule", taskId);
              if (binding === undefined) {
                yield* admit(target);
                return;
              }
              yield* validate(binding, target);
              yield* write("thread", target.threadId, binding);
            }),
          ),
        ),
    });
  }),
);
