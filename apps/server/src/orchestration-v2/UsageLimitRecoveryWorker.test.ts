import { describe, it, expect } from "vite-plus/test";
import {
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
  RunId,
  ServerSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { cloudFallbackCommand } from "./UsageLimitRecoveryWorker.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
const decodeSettings = Schema.decodeSync(ServerSettings);
const source = ProviderInstanceId.make("localClaude"),
  target = ProviderInstanceId.make("cloudClaude");
const instances = [
  {
    instanceId: source,
    enabled: true,
    driverKind: ProviderDriverKind.make("claudeAgent"),
    continuationIdentity: {
      driverKind: ProviderDriverKind.make("claudeAgent"),
      continuationKey: "claude:/account",
    },
  },
  {
    instanceId: target,
    enabled: true,
    driverKind: ProviderDriverKind.make("claudeAgent"),
    continuationIdentity: {
      driverKind: ProviderDriverKind.make("claudeAgent"),
      continuationKey: "claude:/account:cloud-local-v2:cloudClaude",
    },
  },
];
const settings = decodeSettings({
  providerInstances: {
    [source]: { driver: "claudeAgent", enabled: true, config: { cloudFallbackOnLimit: true } },
    [target]: { driver: "claudeAgent", enabled: true, config: { cloudFreeCreditsOnly: true } },
  },
});
const thread = {
  ...liveThreadShell(ThreadId.make("fallback-test")),
  providerInstanceId: source,
  status: "failed" as const,
  lastErrorClass: "usage_limit" as const,
  latestRunId: RunId.make("blocked-run"),
  usageLimitResetAt: null,
  latestRunCompletedAt: null,
  limitRecovery: null,
  snoozedUntil: null,
};
describe("free cloud quota handoff", () => {
  it("selects matching cloud Opus without waiting for a reset time and uses a stable dispatch identity", () => {
    const command = cloudFallbackCommand(thread, settings, instances);
    expect(command).toMatchObject({
      type: "message.dispatch",
      cloudFallbackOfRunId: thread.latestRunId,
      modelSelection: { instanceId: target, model: "claude-opus-5-5" },
    });
    expect(cloudFallbackCommand(thread, settings, instances)).toEqual(command);
  });
  it.each([
    { ...thread, lastErrorClass: "provider_error" as const },
    { ...thread, status: "running" as const },
    { ...thread, runtimeMode: "approval-required" as const },
    { ...thread, interactionMode: "plan" as const },
    { ...thread, settledOverride: "settled" as const },
    { ...thread, latestRunId: null },
    { ...thread, providerInstanceId: target },
  ])("does not turn unrelated or restricted states into a cloud run", (candidate) => {
    expect(cloudFallbackCommand(candidate, settings, instances)).toBeNull();
  });
  it("never switches to another account or a disabled target", () => {
    expect(
      cloudFallbackCommand(thread, settings, [
        instances[0]!,
        {
          ...instances[1]!,
          continuationIdentity: {
            ...instances[1]!.continuationIdentity,
            continuationKey: "another-account",
          },
        },
      ]),
    ).toBeNull();
    expect(
      cloudFallbackCommand(thread, settings, [instances[0]!, { ...instances[1]!, enabled: false }]),
    ).toBeNull();
  });
  it("requires explicit local opt-in", () => {
    const off = decodeSettings({
      providerInstances: {
        ...settings.providerInstances,
        [source]: { driver: "claudeAgent", config: {} },
      },
    });
    expect(cloudFallbackCommand(thread, off, instances)).toBeNull();
  });
  it("does not bypass a project-specific provider disable", () => {
    const off = {
      ...settings,
      projectSettingsOverrides: {
        [thread.projectId]: { providerInstances: { [target]: { enabled: false } } },
      },
    };
    expect(cloudFallbackCommand(thread, off, instances)).toBeNull();
  });
});
