import { describe, it, expect } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, IsoDateTime, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Config from "../config.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Project from "../project/ProjectService.ts";
import * as PullRequests from "../pullRequest/PullRequestService.ts";
import * as Known from "../workspace/KnownWorkspaces.ts";
import * as Files from "../workspace/WorkspaceFileSystem.ts";
import * as Entries from "../workspace/WorkspaceEntries.ts";
import * as Paths from "../workspace/WorkspacePaths.ts";
import * as Vcs from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as Workspace from "./AgentWorkspaceService.ts";
import { liveThreadShell } from "./McpToolAccess.testkit.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const dir = yield* fs.makeTempDirectoryScoped();
  const git = yield* Git.GitVcsDriver.pipe(
    Effect.provide(
      Git.layer.pipe(
        Layer.provide(VcsProcess.layer),
        Layer.provide(Config.ServerConfig.layerTest(dir, `${dir}/state`)),
      ),
    ),
  );
  yield* git.execute({ operation: "test", cwd: dir, args: ["init", "-q"] });
  yield* git.execute({
    operation: "test",
    cwd: dir,
    args: [
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.invalid",
      "commit",
      "--allow-empty",
      "-qm",
      "initial",
    ],
  });
  let shell = liveThreadShell(ThreadId.make("workspace-tools-test"));
  const date = IsoDateTime.make("2026-10-18T00:00:00Z");
  const project = {
    id: shell.projectId,
    title: "Fixture",
    workspaceRoot: dir,
    repositoryIdentity: null,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    scripts: [],
    createdAt: date,
    updatedAt: date,
    deletedAt: null,
  };
  const prCalls: unknown[] = [];
  const scope: McpInvocationScope = {
    environmentId: EnvironmentId.make("test-env"),
    capabilities: new Set(["orchestration", "pull-requests"]),
    issuedAt: 0,
    requestNamespace: "test",
    thread: {
      threadId: shell.id,
      providerSessionId: "test",
      providerInstanceId: shell.providerInstanceId,
    },
    client: undefined,
  };
  const entries = Entries.layer.pipe(Layer.provide(Paths.layer));
  const files = Files.layer.pipe(
    Layer.provide(entries),
    Layer.provide(Paths.layer),
    Layer.provide(Layer.mock(Known.KnownWorkspaces)({ assertContains: () => Effect.void })),
  );
  const layer = Workspace.layer.pipe(
    Layer.provide(
      Layer.mock(PullRequests.PullRequestService)({
        list: (input) => {
          prCalls.push(input);
          return Effect.succeed({
            viewers: {},
            providers: [],
            entries: [],
            errors: [],
            truncated: false,
            nextCursors: {},
          });
        },
        detail: (input) => {
          prCalls.push(input);
          return Effect.die("Unused detail");
        },
      }),
    ),
    Layer.provide(files),
    Layer.provide(entries),
    Layer.provide(Paths.layer),
    Layer.provide(
      Layer.mock(Project.ProjectService)({ getById: () => Effect.succeed(Option.some(project)) }),
    ),
    Layer.provide(
      Layer.mock(Threads.ThreadManagementService)({ getThreadShell: () => Effect.succeed(shell) }),
    ),
    Layer.provide(Vcs.layer.pipe(Layer.provide(VcsProcess.layer))),
    Layer.provide(Git.layer.pipe(Layer.provide(VcsProcess.layer))),
    Layer.provide(Config.ServerConfig.layerTest(dir, `${dir}/state`)),
  );
  const service = yield* Workspace.AgentWorkspaceService.pipe(Effect.provide(layer));
  return {
    service,
    scope,
    fs,
    dir,
    prCalls,
    git,
    setShell: (patch: Partial<typeof shell>) => {
      shell = { ...shell, ...patch };
    },
  };
});
const withServices = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  program.pipe(Effect.provide(NodeServices.layer), Effect.scoped);

describe("agent local workspace", () => {
  it.effect.each(["missing", "unrelated", "replaced"] as const)(
    "refuses a %s worktree before exposing workspace tools",
    (kind) =>
      withServices(
        Effect.gen(function* () {
          const f = yield* fixture;
          const worktree = `${f.dir}/worktree`;
          if (kind === "replaced") {
            yield* f.git.execute({
              operation: "test",
              cwd: f.dir,
              args: ["worktree", "add", "-qb", "original", worktree],
            });
            yield* f.fs.remove(worktree, { recursive: true });
          }
          if (kind !== "missing") {
            yield* f.fs.makeDirectory(worktree);
            yield* f.git.execute({ operation: "test", cwd: worktree, args: ["init", "-q"] });
          }
          f.setShell({ worktreePath: worktree });
          expect((yield* f.service.info(f.scope).pipe(Effect.result))._tag).toBe("Failure");
          expect(
            (yield* f.service.exec(f.scope, { command: "must not execute" }).pipe(Effect.result))
              ._tag,
          ).toBe("Failure");
          expect(
            yield* f.fs.readDirectory(`${f.dir}/state/userdata/workspace-command-jobs`),
          ).toEqual([]);
        }),
      ),
  );
  it.effect("identifies the bound worktree and lists PRs with the existing project account", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        const worktree = `${f.dir}/worktree`;
        yield* f.git.execute({
          operation: "test",
          cwd: f.dir,
          args: ["worktree", "add", "-qb", "test/pr-question", worktree],
        });
        f.setShell({ worktreePath: worktree, branch: "test/pr-question" });
        expect(yield* f.service.info(f.scope)).toMatchObject({
          projectName: "Fixture",
          workspaceRoot: f.dir,
          cwd: worktree,
          branch: "test/pr-question",
        });
        yield* f.service.pullRequests(f.scope, {
          query: "documentation",
          limit: 10,
          filters: { labels: [["documentation"]] },
          cursors: { "github.com": "next-page" },
        });
        expect(f.prCalls).toEqual([
          {
            projectId: liveThreadShell(f.scope.thread!.threadId).projectId,
            state: "all",
            query: "documentation",
            limit: 10,
            filters: { labels: [["documentation"]] },
            cursors: { "github.com": "next-page" },
          },
        ]);
        expect(
          (yield* f.service
            .pullRequests({ ...f.scope, capabilities: new Set(["orchestration"]) }, {})
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(f.prCalls).toHaveLength(1);
      }),
    ),
  );
  it.effect("writes the actual local file and refuses stale versions", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.fs.writeFileString(`${f.dir}/file.txt`, "original");
        const read = yield* f.service.read(f.scope, { path: "file.txt" });
        yield* f.fs.writeFileString(`${f.dir}/file.txt`, "user edit");
        expect(
          (yield* f.service
            .write(f.scope, {
              path: "file.txt",
              contents: "agent edit",
              expectedVersion: read.version,
            })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(yield* f.fs.readFileString(`${f.dir}/file.txt`)).toBe("user edit");
        const current = yield* f.service.read(f.scope, { path: "file.txt" });
        yield* f.service.write(f.scope, {
          path: "file.txt",
          contents: "accepted",
          expectedVersion: current.version,
        });
        expect(yield* f.fs.readFileString(`${f.dir}/file.txt`)).toBe("accepted");
      }),
    ),
  );
  it.effect("creates new files without overwriting existing files", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.service.write(f.scope, {
          path: "new.txt",
          contents: "new",
          expectedVersion: null,
        });
        expect(
          (yield* f.service
            .write(f.scope, { path: "new.txt", contents: "overwrite", expectedVersion: null })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
      }),
    ),
  );
  it.effect("confines file reads and writes to workspace-relative paths", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        for (const path of ["../outside", "/etc/passwd", ".git/config", "C:\\secret"]) {
          expect((yield* f.service.read(f.scope, { path }).pipe(Effect.result))._tag).toBe(
            "Failure",
          );
          expect(
            (yield* f.service
              .write(f.scope, { path, contents: "blocked", expectedVersion: null })
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
        }
      }),
    ),
  );
  it.effect("rejects commands and writes after a stop or mode downgrade", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        f.setShell({ activeRunId: null, status: "idle" });
        expect((yield* f.service.exec(f.scope, { command: "work" }).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        f.setShell({
          ...liveThreadShell(f.scope.thread!.threadId),
          runtimeMode: "approval-required",
        });
        expect(
          (yield* f.service
            .write(f.scope, { path: "file", contents: "bad", expectedVersion: null })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(yield* f.fs.readDirectory(`${f.dir}/state/userdata/workspace-command-jobs`)).toEqual(
          [],
        );
      }),
    ),
  );
  it.live("revalidates job reads, lists, and cancellation after a mode downgrade", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        const job = yield* f.service.exec(f.scope, { command: "printf allowed", yieldMs: 10000 });
        expect((yield* f.service.jobList(f.scope, {})).map((entry) => entry.jobId)).toEqual([
          job.jobId,
        ]);
        f.setShell({ runtimeMode: "approval-required" });
        for (const effect of [
          f.service.jobRead(f.scope, { jobId: job.jobId }).pipe(Effect.asVoid),
          f.service.jobList(f.scope, {}).pipe(Effect.asVoid),
          f.service.jobCancel(f.scope, { jobId: job.jobId }).pipe(Effect.asVoid),
        ])
          expect((yield* effect.pipe(Effect.result))._tag).toBe("Failure");
      }),
    ),
  );
  it.live("pages files consistently and runs commands in the thread's workspace", () =>
    withServices(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.fs.writeFileString(`${f.dir}/file.txt`, "abcdefgh");
        const one = yield* f.service.read(f.scope, { path: "file.txt", maxChars: 3 });
        const two = yield* f.service.read(f.scope, {
          path: "file.txt",
          offset: one.nextOffset!,
          maxChars: 3,
        });
        expect([one.contents, two.contents]).toEqual(["abc", "def"]);
        expect(two.version).toBe(one.version);
        const command = yield* f.service.exec(f.scope, { command: "pwd", yieldMs: 10000 });
        expect(command.exitCode).toBe(0);
        expect(command.stdout.trim()).toBe(f.dir);
      }),
    ),
  );
});
