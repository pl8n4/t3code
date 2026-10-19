import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ClaudeCloudExecutionPolicy } from "./ClaudeCloudExecutionPolicy.ts";

export const layer = Layer.succeed(ClaudeCloudExecutionPolicy, {
  isGuarded: () => Effect.succeed(false),
  guardTool: () => Effect.void,
  admit: () => Effect.succeed(undefined),
  inherit: () => Effect.void,
  assertRelated: () => Effect.void,
  assertScheduleRelated: () => Effect.void,
  bindSchedule: () => Effect.void,
  prepareScheduled: () => Effect.void,
});
