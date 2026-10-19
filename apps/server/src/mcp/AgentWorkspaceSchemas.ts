import { ProjectId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

// Shared with the cloud transport without importing the service and its provider-registry cycle.
export const WorkspaceInfo = Schema.Struct({
  projectId: ProjectId,
  projectName: Schema.String,
  workspaceRoot: Schema.String,
  cwd: Schema.String,
  branch: Schema.NullOr(Schema.String),
  repository: Schema.NullOr(Schema.String),
});

export const WorkspaceExec = Schema.Struct({
  command: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(20000)),
  requestId: Schema.optional(Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(200))),
  yieldMs: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10000 }))),
  timeoutMs: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 604800000 })),
  ),
});
export const WorkspaceJobRead = Schema.Struct({
  jobId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  stdoutOffset: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  stderrOffset: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  maxBytes: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 24000 }))),
  waitMs: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 30000 }))),
});
export const WorkspaceJobId = Schema.Struct({ jobId: WorkspaceJobRead.fields.jobId });
export const WorkspaceJobList = Schema.Struct({
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
});
export const WorkspaceCommandJobState = Schema.Literals([
  "starting",
  "running",
  "completed",
  "cancelled",
  "timed_out",
  "failed",
  "interrupted",
]);
export const WorkspaceCommandJob = Schema.Struct({
  command: Schema.String,
  jobId: WorkspaceJobRead.fields.jobId,
  status: WorkspaceCommandJobState,
  stdout: Schema.String,
  stderr: Schema.String,
  exitCode: Schema.NullOr(Schema.Number),
  timedOut: Schema.Boolean,
  truncated: Schema.Boolean,
  stdoutOffset: Schema.Number,
  stderrOffset: Schema.Number,
  nextStdoutOffset: Schema.Number,
  nextStderrOffset: Schema.Number,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});
export const WorkspaceCommandJobSummary = Schema.Struct({
  command: Schema.String,
  jobId: WorkspaceCommandJob.fields.jobId,
  status: WorkspaceCommandJob.fields.status,
  exitCode: WorkspaceCommandJob.fields.exitCode,
  timedOut: WorkspaceCommandJob.fields.timedOut,
  createdAt: WorkspaceCommandJob.fields.createdAt,
  updatedAt: WorkspaceCommandJob.fields.updatedAt,
});
