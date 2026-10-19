import {
  OrchestratorMcpFailure,
  PullRequestListState,
  PullRequestListFilters,
  PullRequestListCursors,
  type PullRequestListResult,
  type PullRequestListEntry,
  type PullRequestDetail,
  type ProjectListEntriesResult,
  type ProjectReadFileResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Project from "../project/ProjectService.ts";
import * as Processes from "../processRunner.ts";
import * as PullRequests from "../pullRequest/PullRequestService.ts";
import * as Files from "../workspace/WorkspaceFileSystem.ts";
import * as Entries from "../workspace/WorkspaceEntries.ts";
import * as Invocation from "./McpInvocationContext.ts";
import { WorkspaceInfo } from "./AgentWorkspaceSchemas.ts";
import { assertFullAccess, assertLiveCaller, readCaller, unavailable } from "./threadAccess.ts";

export const WorkspaceRead = Schema.Struct({
  path: Schema.String.check(Schema.isNonEmpty()),
  offset: Schema.optional(Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
  maxChars: Schema.optional(
    Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 24000 })),
  ),
});
export const WorkspaceWrite = Schema.Struct({
  path: Schema.String.check(Schema.isNonEmpty()),
  contents: Schema.String.check(Schema.isMaxLength(1_000_000)),
  expectedVersion: Schema.NullOr(Schema.String),
});
export const WorkspaceExec = Schema.Struct({
  command: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(20000)),
});
export const WorkspaceList = Schema.Struct({ path: Schema.optional(Schema.String) });
export const WorkspacePullRequests = Schema.Struct({
  state: Schema.optional(PullRequestListState),
  filters: Schema.optional(PullRequestListFilters),
  cursors: Schema.optional(PullRequestListCursors),
  query: Schema.optional(Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(200))),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 }))),
});
export const WorkspacePullRequestRead = Schema.Struct({
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  repository: Schema.String.check(Schema.isNonEmpty()),
  host: Schema.optional(Schema.String.check(Schema.isNonEmpty())),
});
type WorkspacePullRequestList = Omit<PullRequestListResult, "entries"> & {
  entries: ReadonlyArray<
    Pick<
      PullRequestListEntry,
      | "number"
      | "title"
      | "state"
      | "url"
      | "host"
      | "repository"
      | "headBranch"
      | "updatedAt"
      | "isDraft"
      | "checksState"
    > & { author: { login: string } | null; labels: ReadonlyArray<{ name: string }> }
  >;
};

type Request<A> = Effect.Effect<A, OrchestratorMcpFailure>;
export class AgentWorkspaceService extends Context.Service<
  AgentWorkspaceService,
  {
    readonly info: (scope: Invocation.McpInvocationScope) => Request<typeof WorkspaceInfo.Type>;
    readonly pullRequests: (
      scope: Invocation.McpInvocationScope,
      input: typeof WorkspacePullRequests.Type,
    ) => Request<WorkspacePullRequestList>;
    readonly pullRequestRead: (
      scope: Invocation.McpInvocationScope,
      input: typeof WorkspacePullRequestRead.Type,
    ) => Request<PullRequestDetail>;
    readonly list: (
      scope: Invocation.McpInvocationScope,
      input: typeof WorkspaceList.Type,
    ) => Request<ProjectListEntriesResult>;
    readonly read: (
      scope: Invocation.McpInvocationScope,
      input: typeof WorkspaceRead.Type,
    ) => Request<ProjectReadFileResult & { version: string; nextOffset: number | null }>;
    readonly write: (
      scope: Invocation.McpInvocationScope,
      input: typeof WorkspaceWrite.Type,
    ) => Request<{ path: string; version: string }>;
    readonly exec: (
      scope: Invocation.McpInvocationScope,
      input: typeof WorkspaceExec.Type,
    ) => Request<{
      stdout: string;
      stderr: string;
      exitCode: number | null;
      timedOut: boolean;
      truncated: boolean;
    }>;
  }
>()("t3/mcp/AgentWorkspaceService") {}

const make = Effect.gen(function* () {
  const threads = yield* Threads.ThreadManagementService;
  const projects = yield* Project.ProjectService;
  const files = yield* Files.WorkspaceFileSystem;
  const entries = yield* Entries.WorkspaceEntries;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const processes = yield* Processes.ProcessRunner;
  const pullRequests = yield* PullRequests.PullRequestService;
  const workspace = (scope: Invocation.McpInvocationScope, write = false) =>
    Effect.gen(function* () {
      const context = yield* readCaller().pipe(
        Effect.provideService(Threads.ThreadManagementService, threads),
        Effect.provideService(Invocation.McpInvocationContext, scope),
      );
      yield* assertLiveCaller(context);
      if (!context.caller)
        return yield* new OrchestratorMcpFailure({
          code: "thread_credential_required",
          message: "A live T3 thread is required.",
        });
      if (write)
        yield* assertFullAccess(
          context,
          "Local workspace writes and commands require Full access in a default-mode thread.",
        );
      const project = yield* projects
        .getById(context.caller.projectId)
        .pipe(Effect.mapError(unavailable));
      if (Option.isNone(project))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The calling thread's project is unavailable.",
        });
      return {
        projectId: project.value.id,
        projectName: project.value.title,
        workspaceRoot: project.value.workspaceRoot,
        cwd: context.caller.worktreePath ?? project.value.workspaceRoot,
        branch: context.caller.branch,
        repository: project.value.repositoryIdentity?.canonicalKey ?? null,
      };
    });
  const root = (scope: Invocation.McpInvocationScope, write = false) =>
    workspace(scope, write).pipe(Effect.map((value) => value.cwd));
  const prWorkspace = (scope: Invocation.McpInvocationScope) =>
    scope.capabilities.has("pull-requests")
      ? workspace(scope)
      : Effect.fail(
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "This credential cannot read pull requests.",
          }),
        );
  const relative = (value: string) => {
    const segments = value.replaceAll("\\", "/").split("/");
    return (
      !path.isAbsolute(value) &&
      !/^[A-Za-z]:/.test(value) &&
      !segments.some((s) => s === ".." || s.toLowerCase() === ".git")
    );
  };
  const checkPath = (value: string) =>
    relative(value)
      ? Effect.void
      : Effect.fail(
          new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Use a path inside the calling thread's workspace, outside .git.",
          }),
        );
  const version = (text: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(text))
      .pipe(Effect.map(Hex.encode), Effect.mapError(unavailable));

  return AgentWorkspaceService.of({
    info: Effect.fn("AgentWorkspaceService.info")(workspace),
    pullRequests: Effect.fn("AgentWorkspaceService.pullRequests")(function* (scope, input) {
      const current = yield* prWorkspace(scope);
      const result = yield* pullRequests
        .list({
          projectId: current.projectId,
          state: input.state ?? "all",
          limit: input.limit ?? 25,
          ...(input.query ? { query: input.query } : {}),
          ...(input.filters ? { filters: input.filters } : {}),
          ...(input.cursors ? { cursors: input.cursors } : {}),
        })
        .pipe(Effect.mapError(unavailable));
      return {
        ...result,
        entries: result.entries.map((entry) => ({
          number: entry.number,
          title: entry.title,
          state: entry.state,
          url: entry.url,
          host: entry.host,
          repository: entry.repository,
          headBranch: entry.headBranch,
          updatedAt: entry.updatedAt,
          isDraft: entry.isDraft,
          checksState: entry.checksState,
          author: entry.author ? { login: entry.author.login } : null,
          labels: entry.labels.map(({ name }) => ({ name })),
        })),
      };
    }),
    pullRequestRead: Effect.fn("AgentWorkspaceService.pullRequestRead")(function* (scope, input) {
      const current = yield* prWorkspace(scope);
      return yield* pullRequests
        .detail({ projectId: current.projectId, ...input })
        .pipe(Effect.mapError(unavailable));
    }),
    list: Effect.fn("AgentWorkspaceService.list")(function* (scope, input) {
      const cwd = yield* root(scope);
      yield* checkPath(input.path ?? "");
      return yield* entries
        .list({ cwd, directoryPath: input.path ?? "" })
        .pipe(Effect.mapError(unavailable));
    }),
    read: Effect.fn("AgentWorkspaceService.read")(function* (scope, input) {
      const cwd = yield* root(scope);
      yield* checkPath(input.path);
      const file = yield* files
        .readFile({ cwd, relativePath: input.path })
        .pipe(Effect.mapError(unavailable));
      const offset = input.offset ?? 0;
      const contents = file.contents.slice(offset, offset + (input.maxChars ?? 16000));
      return {
        ...file,
        contents,
        version: yield* version(file.contents),
        nextOffset:
          offset + contents.length < file.contents.length ? offset + contents.length : null,
      };
    }),
    write: Effect.fn("AgentWorkspaceService.write")(function* (scope, input) {
      const cwd = yield* root(scope, true);
      yield* checkPath(input.path);
      const exists = yield* fs
        .exists(path.join(cwd, input.path))
        .pipe(Effect.mapError(unavailable));
      const current = exists
        ? yield* files
            .readFile({ cwd, relativePath: input.path })
            .pipe(Effect.mapError(unavailable))
        : null;
      if (
        current?.truncated ||
        (current === null ? null : yield* version(current.contents)) !== input.expectedVersion
      )
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "The file changed since it was read. Read it again before writing; use expectedVersion=null only for a new file.",
        });
      yield* files
        .writeFile({ cwd, relativePath: input.path, contents: input.contents })
        .pipe(Effect.mapError(unavailable));
      return { path: input.path, version: yield* version(input.contents) };
    }),
    exec: Effect.fn("AgentWorkspaceService.exec")(function* (scope, input) {
      const cwd = yield* root(scope, true);
      const result = yield* processes
        .run({
          command: "sh",
          args: ["-lc", input.command],
          cwd,
          timeout: "60 seconds",
          timeoutBehavior: "timedOutResult",
          maxOutputBytes: 16000,
          outputMode: "truncate",
        })
        .pipe(Effect.mapError(unavailable));
      return {
        stdout: result.stdout,
        stderr: result.stderr,
        exitCode: result.code,
        timedOut: result.timedOut,
        truncated: result.stdoutTruncated || result.stderrTruncated,
      };
    }),
  });
});
export const layer = Layer.effect(AgentWorkspaceService, make);
