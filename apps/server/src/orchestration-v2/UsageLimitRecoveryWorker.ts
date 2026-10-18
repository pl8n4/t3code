import {
  CommandId,
  MessageId,
  type OrchestrationV2Command,
  type ServerSettings as ServerSettingsValue,
  ClaudeSettings,
  resolveProjectProviderInstanceEnabled,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as Instances from "../provider/ProviderInstanceRegistry.ts";
import { CLAUDE_CLOUD_MODEL } from "../provider/ClaudeCloudPolicy.ts";

const decodeClaudeConfig = Schema.decodeUnknownOption(ClaudeSettings);

export function cloudFallbackCommand(
  thread: ProjectionStore.ProjectionLimitRecoveryCandidate,
  settings: ServerSettingsValue,
  instances: ReadonlyArray<
    Pick<ProviderInstance, "instanceId" | "driverKind" | "enabled" | "continuationIdentity">
  >,
): OrchestrationV2Command | null {
  const source = instances.find((i) => i.instanceId === thread.providerInstanceId);
  const entry = settings.providerInstances[thread.providerInstanceId];
  const config = entry?.driver === "claudeAgent" ? decodeClaudeConfig(entry.config) : Option.none();
  if (
    thread.status !== "failed" ||
    thread.lastErrorClass !== "usage_limit" ||
    !thread.latestRunId ||
    thread.archivedAt !== null ||
    thread.settledOverride === "settled" ||
    thread.pendingRuntimeRequest !== null ||
    thread.snoozedUntil != null ||
    thread.runtimeMode !== "full-access" ||
    thread.interactionMode !== "default" ||
    !source?.enabled ||
    source.driverKind !== "claudeAgent" ||
    Option.isNone(config) ||
    config.value.cloudFallbackOnLimit !== true ||
    config.value.cloudFreeCreditsOnly
  )
    return null;
  const targets = instances.filter((i) => {
    if (
      i.instanceId === source.instanceId ||
      !i.enabled ||
      i.driverKind !== "claudeAgent" ||
      Option.getOrUndefined(decodeClaudeConfig(settings.providerInstances[i.instanceId]?.config))
        ?.cloudFreeCreditsOnly !== true ||
      !resolveProjectProviderInstanceEnabled(settings, thread.projectId, i.instanceId)
    )
      return false;
    const suffix = `:cloud-local-v2:${i.instanceId}`;
    return (
      i.continuationIdentity.continuationKey ===
      source.continuationIdentity.continuationKey + suffix
    );
  });
  if (targets.length !== 1) return null;
  const identity = `cloud-fallback:${thread.id}:${thread.latestRunId}`;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(identity),
    messageId: MessageId.make(identity),
    threadId: thread.id,
    cloudFallbackOfRunId: thread.latestRunId,
    modelSelection: { instanceId: targets[0]!.instanceId, model: CLAUDE_CLOUD_MODEL },
    text: "Local Claude reached its subscription usage limit. Continue the user's unfinished task using promotional cloud credit and T3's local workspace tools. Inspect current workspace changes first, preserve user edits, and do not repeat actions that already succeeded.",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "server",
  };
}

export function limitRecoveryCommand(
  thread: ProjectionStore.ProjectionLimitRecoveryCandidate,
  autoResume: boolean,
  nowMs: number,
  snooze = false,
): OrchestrationV2Command | null {
  if (
    thread.status !== "failed" ||
    thread.lastErrorClass !== "usage_limit" ||
    !thread.latestRunId ||
    !thread.usageLimitResetAt ||
    thread.archivedAt !== null ||
    thread.settledOverride === "settled" ||
    thread.pendingRuntimeRequest !== null
  )
    return null;
  const resetMs = Date.parse(thread.usageLimitResetAt);
  // An already-expired window reported with a fresh failure cannot start a retry loop.
  if (
    !Number.isFinite(resetMs) ||
    resetMs <= DateTime.toEpochMillis(thread.latestRunCompletedAt ?? thread.updatedAt)
  )
    return null;
  const identity = `${thread.id}:${thread.latestRunId}:${resetMs}`;
  const recovery = thread.limitRecovery;
  if (recovery?.runId !== thread.latestRunId || recovery.resetAt !== thread.usageLimitResetAt) {
    if (!autoResume && (!snooze || resetMs <= nowMs)) return null;
    return {
      type: "thread.metadata.update",
      commandId: CommandId.make(`limit-arm:${identity}`),
      threadId: thread.id,
      limitRecovery: {
        runId: thread.latestRunId,
        resetAt: thread.usageLimitResetAt,
        autoResume,
        snooze: snooze && resetMs > nowMs,
      },
    };
  }
  if (
    !recovery.autoResume ||
    resetMs > nowMs ||
    (thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs)
  )
    return null;
  const deliveryIdentity = `${identity}:${recovery.requestId ?? "legacy"}`;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(`limit-resume:${deliveryIdentity}`),
    messageId: MessageId.make(`limit-resume:${deliveryIdentity}`),
    threadId: thread.id,
    usageLimitContinuationOfRunId: thread.latestRunId,
    ...(recovery.requestId === undefined
      ? {}
      : { usageLimitRecoveryRequestId: recovery.requestId }),
    text: "Continue where you left off.",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "server",
  };
}

const makeSweep = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const settings = yield* ServerSettings.ServerSettingsService;
  const registry = yield* Effect.serviceOption(Instances.ProviderInstanceRegistry);
  return Effect.fn("UsageLimitRecoveryWorker.sweep")(function* () {
    const preferences = yield* settings.getSettings;
    const now = yield* DateTime.now;
    const instances = Option.isSome(registry) ? yield* registry.value.listInstances : [];
    const cloudFallback = Object.values(preferences.providerInstances).some(
      (i) =>
        i.driver === "claudeAgent" &&
        Option.getOrUndefined(decodeClaudeConfig(i.config))?.cloudFallbackOnLimit === true,
    );
    const candidates = yield* projections.getLimitRecoveryCandidates({
      now,
      autoResume: preferences.autoResumeLimitedThreads,
      snooze: preferences.snoozeLimitedThreads,
      cloudFallback,
    });
    const nowMs = DateTime.toEpochMillis(now);
    for (const thread of candidates) {
      const command =
        cloudFallbackCommand(thread, preferences, instances) ??
        limitRecoveryCommand(
          thread,
          preferences.autoResumeLimitedThreads,
          nowMs,
          preferences.snoozeLimitedThreads,
        );
      if (command === null) continue;
      yield* threads.dispatch(command).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("orchestration-v2.limit-recovery.dispatch-failed", {
            threadId: thread.id,
            cause,
          }),
        ),
      );
    }
  });
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const sweep = yield* makeSweep;
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("usage-limit-recovery", sweep());
  }),
);
