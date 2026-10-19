import { describe, expect, it } from "vite-plus/test";
import {
  orderedClaudeCloudEvents,
  verifyClaudeCloudCredit,
  verifyClaudeCloudEnvironment,
  verifyClaudeCloudModel,
} from "./ClaudeCloudPolicy.ts";

const now = Date.parse("2026-10-18T22:00:00Z");
const credit = {
  limit_dollars: 100,
  remaining_dollars: 99.8,
  resets_at: "2026-11-05T07:59:00+00:00",
  locked_reason: null,
};
const usage = {
  extra_usage: { is_enabled: false },
  spend: { enabled: false },
  iguana_necktie: credit,
};

describe("Claude cloud free-credit admission", () => {
  it("keeps promotional dollars distinct from purchased usage credits", () => {
    expect(verifyClaudeCloudCredit(usage, now).remainingDollars).toBe(99.8);
  });
  it.each([
    null,
    {},
    { ...usage, extra_usage: null },
    { ...usage, extra_usage: { is_enabled: true } },
    { ...usage, spend: { enabled: true } },
    { ...usage, spend: undefined },
    { ...usage, iguana_necktie: null },
    { ...usage, iguana_necktie: { ...credit, remaining_dollars: "99.8" } },
  ])("blocks unknown billing or enabled paid usage (%j)", (input) => {
    expect(() => verifyClaudeCloudCredit(input, now)).toThrow();
  });
  it.each([0, -1, 101, Infinity, NaN])("blocks invalid or exhausted balances: %s", (remaining) => {
    expect(() =>
      verifyClaudeCloudCredit(
        { ...usage, iguana_necktie: { ...credit, remaining_dollars: remaining } },
        now,
      ),
    ).toThrow();
  });
  it.each(["not-a-date", "2026-10-18T21:59:00Z", "2026-10-18T22:00:15Z"])(
    "blocks expired, imminent, and unknown expiries (%s)",
    (expiry) => {
      expect(() =>
        verifyClaudeCloudCredit(
          { ...usage, iguana_necktie: { ...credit, resets_at: expiry } },
          now,
        ),
      ).toThrow();
    },
  );
  it("blocks provider-locked promotional credit", () => {
    expect(() =>
      verifyClaudeCloudCredit(
        { ...usage, iguana_necktie: { ...credit, locked_reason: "unavailable" } },
        now,
      ),
    ).toThrow();
  });
  it.each([
    "sonnet",
    "claude-fable-5",
    "claude-opus-4-6",
    "haiku[fast]",
    "default",
    "best",
    "custom-model",
  ])("rejects unsupported model %s before a cloud request", (model) => {
    expect(() => verifyClaudeCloudModel(model)).toThrow();
  });
  it.each([
    ["haiku", "claude-haiku-5-5"],
    ["claude-haiku-5-5", "claude-haiku-5-5"],
    ["opus", "claude-opus-5-5"],
    ["claude-opus-5-5", "claude-opus-5-5"],
    ["fable", "claude-fable-5-1"],
    ["claude-fable-5-1", "claude-fable-5-1"],
  ])("pins %s to the supported cloud model %s", (model, canonical) => {
    expect(verifyClaudeCloudModel(model)).toBe(canonical);
  });
  it.each([
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
  ])("rejects alternate billing configuration %s", (key) => {
    expect(() => verifyClaudeCloudEnvironment({ [key]: "configured" })).toThrow();
  });
  it("permits subscription authentication", () => {
    expect(() =>
      verifyClaudeCloudEnvironment({ CLAUDE_CODE_OAUTH_TOKEN: "not-an-api-key" }),
    ).not.toThrow();
  });
});

describe("Claude cloud event replay", () => {
  it("orders the server's descending page without replaying already acknowledged events", () => {
    const page = {
      data: ["26", "24", "25", "23"].map((sequence_num) => ({
        event_id: `e${sequence_num}`,
        sequence_num,
        payload: { type: "assistant" },
      })),
    };
    expect(orderedClaudeCloudEvents(page, 23n).map((e) => e.sequence)).toEqual([24n, 25n, 26n]);
  });
  it("does not lose precision in large sequence numbers", () => {
    const page = {
      data: [{ event_id: "e", sequence_num: "9007199254740993", payload: { type: "result" } }],
    };
    expect(orderedClaudeCloudEvents(page, 9007199254740992n)[0]?.sequence).toBe(9007199254740993n);
  });
});
