import * as Schema from "effect/Schema";

export const CLAUDE_CLOUD_MODEL = "claude-opus-5-5";
export const CLAUDE_CLOUD_MODELS = [
  "claude-haiku-5-5",
  "claude-opus-5-5",
  "claude-fable-5-1",
] as const;

const Money = Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0));
const Usage = Schema.Struct({
  extra_usage: Schema.Struct({ is_enabled: Schema.Literal(false) }),
  spend: Schema.Struct({ enabled: Schema.Literal(false) }),
  iguana_necktie: Schema.Struct({
    limit_dollars: Money,
    remaining_dollars: Money,
    resets_at: Schema.String,
    locked_reason: Schema.NullOr(Schema.String),
  }),
});

export class ClaudeCloudGuardError extends Schema.TaggedError<ClaudeCloudGuardError>()(
  "ClaudeCloudGuardError",
  {
    reason: Schema.Literals([
      "unverified_billing",
      "no_credit",
      "expired_credit",
      "locked_credit",
      "unsupported_model",
      "model_unavailable",
      "unsupported_policy",
      "api_configuration",
      "account_mismatch",
      "remote_failed",
      "tools_unavailable",
      "session_missing",
      "delivery_pending",
    ]),
  },
) {
  override get message(): string {
    const messages = {
      unverified_billing:
        "Cloud launch blocked: verify promotional credit and turn off paid usage credits in Claude Settings > Usage.",
      no_credit: "Cloud launch blocked: no promotional cloud credit remains.",
      expired_credit: "Cloud launch blocked: promotional credit has expired or is about to expire.",
      locked_credit: "Cloud launch blocked: promotional credit is unavailable.",
      unsupported_model:
        "This free-credit prototype supports Haiku 5.5, Opus 5.5, and Fable 5.1. Select one before sending.",
      model_unavailable:
        "Claude Cloud could not select the requested model. It may be unavailable for this account or promotional balance. No coding prompt was sent and T3 did not enable paid usage.",
      unsupported_policy:
        "This cloud prototype requires Full access. Remote tools cannot use local T3 permission callbacks.",
      api_configuration:
        "Cloud launch blocked: API keys, custom API endpoints, and third-party billing configurations are not supported.",
      account_mismatch:
        "Cloud launch blocked: sign in to the matching Claude subscription in the T3 browser.",
      remote_failed:
        "Claude's cloud interface returned an unsupported response. No automatic retry was made; inspect the cloud session before trying again.",
      tools_unavailable:
        "The cloud session could not connect to T3's local tools. No coding prompt was sent. Reconnect T3 and retry.",
      session_missing:
        "The saved cloud session binding is missing. No replacement conversation was created. Restore the prototype's session data or start a new thread with the existing conversation as context.",
      delivery_pending:
        "Unconfirmed cloud output was preserved in the prototype's recovery journal. Resume is blocked to avoid losing history or repeating local actions. Open the cloud conversation in this thread's Browser panel to recover it, then start a new thread with that context. No new prompt was sent.",
    };
    return messages[this.reason];
  }
}

const decodeUsage = Schema.decodeUnknownOption(Usage);

export function verifyClaudeCloudCredit(raw: unknown, now: number) {
  const decoded = decodeUsage(raw);
  if (decoded._tag === "None") throw new ClaudeCloudGuardError({ reason: "unverified_billing" });
  const credit = decoded.value.iguana_necktie;
  if (credit.locked_reason?.trim()) throw new ClaudeCloudGuardError({ reason: "locked_credit" });
  const expiry = Date.parse(credit.resets_at);
  if (!Number.isFinite(expiry) || expiry <= now + 30_000) {
    throw new ClaudeCloudGuardError({ reason: "expired_credit" });
  }
  if (
    credit.limit_dollars <= 0 ||
    credit.remaining_dollars <= 0 ||
    credit.remaining_dollars > credit.limit_dollars
  ) {
    throw new ClaudeCloudGuardError({ reason: "no_credit" });
  }
  return { remainingDollars: credit.remaining_dollars, expiresAt: credit.resets_at };
}

export function verifyClaudeCloudModel(model: string) {
  const resolved =
    model === "haiku"
      ? "claude-haiku-5-5"
      : model === "opus"
        ? "claude-opus-5-5"
        : model === "fable"
          ? "claude-fable-5-1"
          : model;
  const canonical = CLAUDE_CLOUD_MODELS.find((allowed) => allowed === resolved);
  if (!canonical) throw new ClaudeCloudGuardError({ reason: "unsupported_model" });
  return canonical;
}

export function verifyClaudeCloudEnvironment(
  environment: Readonly<Record<string, string | undefined>>,
) {
  const forbidden = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
  ];
  if (forbidden.some((key) => environment[key]?.trim())) {
    throw new ClaudeCloudGuardError({ reason: "api_configuration" });
  }
}

export const ClaudeCloudEvent = Schema.Struct({
  event_id: Schema.String,
  sequence_num: Schema.String,
  payload: Schema.Record(Schema.String, Schema.Unknown),
});
const ClaudeCloudEventPage = Schema.Struct({ data: Schema.Array(ClaudeCloudEvent) });

const decodeEvents = Schema.decodeUnknownSync(ClaudeCloudEventPage);

export function orderedClaudeCloudEvents(raw: unknown, after: bigint) {
  const page = decodeEvents(raw);
  return page.data
    .map((event) => ({ ...event, sequence: BigInt(event.sequence_num) }))
    .filter((event) => event.sequence > after)
    .toSorted((a, b) => (a.sequence < b.sequence ? -1 : a.sequence > b.sequence ? 1 : 0));
}
