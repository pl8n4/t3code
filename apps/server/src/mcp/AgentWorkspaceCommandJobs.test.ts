import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Register the native watcher before checking the detached process journal, avoiding a lost-event race in this integration fixture.
import * as NodeFS from "node:fs";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Config from "../config.ts";
import { makeCommandJobs } from "./AgentWorkspaceCommandJobs.ts";

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeReceipt = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodeStatus = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ status: Schema.String })),
);
const encodeStaleStatus = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      status: Schema.String,
      exitCode: Schema.NullOr(Schema.Number),
      timedOut: Schema.Boolean,
      updatedAt: Schema.Number,
    }),
  ),
);
const waitForState = (dir: string, statuses: ReadonlyArray<string>) =>
  Effect.callback<void>((resume) => {
    const check = () => {
      try {
        if (
          statuses.includes(decodeStatus(NodeFS.readFileSync(`${dir}/status.json`, "utf8")).status)
        )
          resume(Effect.void);
      } catch {
        // The journal may not exist yet; directory events notify us after the atomic rename.
      }
    };
    const watcher = NodeFS.watch(dir, check);
    check();
    return Effect.sync(() => watcher.close());
  });
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const dir = yield* fs.makeTempDirectoryScoped();
  const config = Config.ServerConfig.layerTest(dir, `${dir}/state`);
  const jobs = yield* makeCommandJobs.pipe(Effect.provide(config));
  const owner = {
    environmentId: "env",
    threadId: "thread",
    projectId: "project",
    cwd: dir,
    workspaceIdentity: "fixture-inode",
  };
  const jobDir = (jobId: string) => `${dir}/state/userdata/workspace-command-jobs/${jobId}`;
  return { fs, dir, jobs, owner, jobDir, config };
});
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer), Effect.scoped);

describe("durable workspace command jobs", () => {
  it.live("returns short command output and pages stdout and stderr", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const result = yield* f.jobs.start(f.owner, {
          command: "printf 123456789; printf warning >&2; exit 7",
          yieldMs: 10000,
        });
        expect(result).toMatchObject({
          status: "completed",
          stdout: "123456789",
          stderr: "warning",
          exitCode: 7,
          timedOut: false,
        });
        const page = yield* f.jobs.read(f.owner, { jobId: result.jobId, maxBytes: 4 });
        expect(page).toMatchObject({
          stdout: "1234",
          stderr: "warn",
          truncated: true,
          nextStdoutOffset: 4,
          nextStderrOffset: 4,
        });
        expect(
          yield* f.jobs.read(f.owner, {
            jobId: result.jobId,
            stdoutOffset: page.nextStdoutOffset,
            stderrOffset: page.nextStderrOffset,
            maxBytes: 4,
          }),
        ).toMatchObject({ stdout: "5678", stderr: "ing" });
      }),
    ),
  );
  it.live("pages Unicode output without splitting a code point", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const job = yield* f.jobs.start(f.owner, { command: "printf '😀é😀end'", yieldMs: 10000 });
        let output = "";
        let offset = 0;
        let page;
        do {
          page = yield* f.jobs.read(f.owner, {
            jobId: job.jobId,
            stdoutOffset: offset,
            maxBytes: 1,
          });
          output += page.stdout;
          offset = page.nextStdoutOffset;
        } while (page.truncated);
        expect(output).toBe("😀é😀end");
      }),
    ),
  );
  it.live("retains a trailing incomplete UTF8 character until the live job writes the rest", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const job = yield* f.jobs.start(f.owner, { command: "printf initial", yieldMs: 10000 });
        const dir = f.jobDir(job.jobId);
        yield* f.fs.writeFileString(
          `${dir}/status.json`,
          yield* encodeStaleStatus({
            status: "running",
            exitCode: null,
            timedOut: false,
            updatedAt: yield* Clock.currentTimeMillis,
          }),
        );
        yield* f.fs.writeFile(`${dir}/stdout.log`, new Uint8Array([0xf0, 0x9f]));
        expect(yield* f.jobs.read(f.owner, { jobId: job.jobId, maxBytes: 1 })).toMatchObject({
          stdout: "",
          nextStdoutOffset: 0,
          truncated: true,
        });
        yield* f.fs.writeFile(`${dir}/stdout.log`, new Uint8Array([0xf0, 0x9f, 0x98, 0x80]));
        expect(yield* f.jobs.read(f.owner, { jobId: job.jobId, maxBytes: 1 })).toMatchObject({
          stdout: "😀",
          nextStdoutOffset: 4,
          truncated: false,
        });
      }),
    ),
  );
  it.live("reports an externally terminated shell as interrupted", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        expect(
          yield* f.jobs.start(f.owner, { command: "kill -TERM $$", yieldMs: 10000 }),
        ).toMatchObject({ status: "interrupted", exitCode: null });
      }),
    ),
  );
  it.live("reattaches after service and request scopes close without repeating a start", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const request = {
          command:
            "printf once >> starts; while [ ! -f release ]; do sleep 0.1; done; printf reattached",
          requestId: "stable-start",
          yieldMs: 0,
        };
        const first = yield* Effect.scoped(f.jobs.start(f.owner, request));
        yield* waitForState(f.jobDir(first.jobId), ["running"]);
        const replacement = yield* makeCommandJobs.pipe(Effect.provide(f.config));
        const second = yield* replacement.start(f.owner, request);
        expect(second.jobId).toBe(first.jobId);
        expect(second.status).toBe("running");
        expect((yield* replacement.list(f.owner, 20)).map((job) => job.jobId)).toEqual([
          first.jobId,
        ]);
        yield* f.fs.writeFileString(`${f.dir}/release`, "");
        yield* waitForState(f.jobDir(first.jobId), ["completed"]);
        expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("once");
        expect(yield* replacement.read(f.owner, { jobId: first.jobId })).toMatchObject({
          status: "completed",
          stdout: "reattached",
        });
        expect(
          yield* replacement
            .start(f.owner, { ...request, command: "different" })
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure" });
      }),
    ),
  );
  it.live("tracks background descendants until their inherited output closes", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const job = yield* f.jobs.start(f.owner, {
          command:
            "sh -c 'while [ ! -f release ]; do sleep 0.1; done; printf background-complete' &",
          yieldMs: 1000,
        });
        expect(job.status).toBe("running");
        yield* f.fs.writeFileString(`${f.dir}/release`, "");
        yield* waitForState(f.jobDir(job.jobId), ["completed"]);
        expect(yield* f.jobs.read(f.owner, { jobId: job.jobId })).toMatchObject({
          status: "completed",
          stdout: "background-complete",
        });
      }),
    ),
  );
  it.live(
    "keeps commands running beyond the old sixty-second deadline",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture;
          const result = yield* f.jobs.start(f.owner, {
            command: "sleep 61; printf long-command-complete",
            yieldMs: 0,
          });
          yield* waitForState(f.jobDir(result.jobId), ["completed"]);
          expect(yield* f.jobs.read(f.owner, { jobId: result.jobId })).toMatchObject({
            status: "completed",
            exitCode: 0,
            timedOut: false,
            stdout: "long-command-complete",
          });
        }),
      ),
    75000,
  );
  it.live("cancels its own shell and descendants while leaving another job running", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const one = yield* f.jobs.start(f.owner, {
          command: "sh -c 'trap \"\" TERM; while :; do sleep 10; done' & wait",
          yieldMs: 0,
        });
        const other = yield* f.jobs.start(f.owner, {
          command: "while [ ! -f release ]; do sleep 0.1; done; printf independent",
          yieldMs: 0,
        });
        yield* waitForState(f.jobDir(one.jobId), ["running"]);
        yield* waitForState(f.jobDir(other.jobId), ["running"]);
        yield* f.jobs.cancel(f.owner, one.jobId);
        yield* waitForState(f.jobDir(one.jobId), ["cancelled"]);
        expect((yield* f.jobs.read(f.owner, { jobId: other.jobId })).status).toBe("running");
        yield* f.fs.writeFileString(`${f.dir}/release`, "");
        yield* waitForState(f.jobDir(other.jobId), ["completed"]);
        expect((yield* f.jobs.read(f.owner, { jobId: other.jobId })).stdout).toBe("independent");
      }),
    ),
  );
  it.live("enforces an explicit deadline without imposing a default one", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const result = yield* f.jobs.start(f.owner, {
          command: "sleep 100",
          timeoutMs: 100,
          yieldMs: 0,
        });
        yield* waitForState(f.jobDir(result.jobId), ["timed_out"]);
        expect(yield* f.jobs.read(f.owner, { jobId: result.jobId })).toMatchObject({
          status: "timed_out",
          timedOut: true,
          exitCode: null,
        });
      }),
    ),
  );
  it.live("does not reveal, cancel, or list another thread's jobs", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const result = yield* f.jobs.start(f.owner, { command: "printf private", yieldMs: 10000 });
        for (const owner of [
          { ...f.owner, threadId: "foreign" },
          { ...f.owner, cwd: "/other" },
          { ...f.owner, environmentId: "foreign" },
          { ...f.owner, workspaceIdentity: "replacement-directory" },
        ]) {
          expect(
            (yield* f.jobs.read(owner, { jobId: result.jobId }).pipe(Effect.result))._tag,
          ).toBe("Failure");
          expect((yield* f.jobs.cancel(owner, result.jobId).pipe(Effect.result))._tag).toBe(
            "Failure",
          );
          expect(yield* f.jobs.list(owner, 10)).toEqual([]);
        }
      }),
    ),
  );
  it.live(
    "refuses the same request after directory replacement, project changes, or worktree rebinding",
    () =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture;
          const cwd = `${f.dir}/workspace`;
          yield* f.fs.makeDirectory(cwd);
          const stat = yield* f.fs.stat(cwd);
          const owner = {
            ...f.owner,
            cwd,
            workspaceIdentity: `${stat.dev}:${stat.ino._tag === "Some" ? stat.ino.value : "unknown"}`,
          };
          const request = {
            command: "printf x >> ../starts",
            requestId: "once-across-workspaces",
            yieldMs: 10000,
          };
          const original = yield* f.jobs.start(owner, request);
          yield* f.fs.rename(cwd, `${f.dir}/old-workspace`);
          yield* f.fs.makeDirectory(cwd);
          const nextStat = yield* f.fs.stat(cwd);
          const changedIdentity = `${nextStat.dev}:${nextStat.ino._tag === "Some" ? nextStat.ino.value : "unknown"}`;
          const replacement = yield* makeCommandJobs.pipe(Effect.provide(f.config));
          for (const changed of [
            { ...owner, workspaceIdentity: changedIdentity },
            { ...owner, projectId: "another-project" },
            { ...owner, cwd: `${f.dir}/old-workspace` },
          ])
            expect((yield* replacement.start(changed, request).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          expect((yield* replacement.start(owner, request)).jobId).toBe(original.jobId);
          expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("x");
          expect(
            yield* f.fs.readDirectory(`${f.dir}/state/userdata/workspace-command-jobs`),
          ).toEqual([original.jobId]);
        }),
      ),
  );
  it.live("refuses an uncertain retry after a workspace symlink changes target", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const one = `${f.dir}/one`;
        const two = `${f.dir}/two`;
        const link = `${f.dir}/workspace`;
        yield* f.fs.makeDirectory(one);
        yield* f.fs.makeDirectory(two);
        yield* f.fs.symlink(one, link);
        const before = {
          ...f.owner,
          cwd: yield* f.fs.realPath(link),
          workspaceIdentity: "inode-one",
        };
        const request = {
          command: "printf x >> ../starts",
          requestId: "through-symlink",
          yieldMs: 10000,
        };
        yield* f.jobs.start(before, request);
        yield* f.fs.remove(link);
        yield* f.fs.symlink(two, link);
        const after = {
          ...before,
          cwd: yield* f.fs.realPath(link),
          workspaceIdentity: "inode-two",
        };
        expect((yield* f.jobs.start(after, request).pipe(Effect.result))._tag).toBe("Failure");
        expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("x");
        expect(
          yield* f.fs.readDirectory(`${f.dir}/state/userdata/workspace-command-jobs`),
        ).toHaveLength(1);
      }),
    ),
  );
  it.live.each([false, true])(
    "recognizes legacy receipts without requestId before a changed-owner retry (%s)",
    (changeOwner) =>
      run(
        Effect.gen(function* () {
          const f = yield* fixture;
          const crypto = yield* Crypto.Crypto;
          const request = {
            command: "printf x >> starts",
            requestId: "phase-two-key",
            yieldMs: 10000,
          };
          const original = yield* f.jobs.start(f.owner, request);
          const receiptPath = `${f.jobDir(original.jobId)}/request.json`;
          const receipt = yield* f.fs
            .readFileString(receiptPath)
            .pipe(Effect.flatMap(decodeReceipt));
          const { requestId: _requestId, ...oldReceipt } = receipt;
          yield* f.fs.writeFileString(receiptPath, yield* encodeJson(oldReceipt));
          const oldId = Hex.encode(
            yield* crypto.digest(
              "SHA-256",
              new TextEncoder().encode(yield* encodeJson([f.owner, request.requestId])),
            ),
          );
          yield* f.fs.rename(f.jobDir(original.jobId), f.jobDir(oldId));
          const replacement = yield* makeCommandJobs.pipe(Effect.provide(f.config));
          if (changeOwner)
            expect(
              (yield* replacement
                .start({ ...f.owner, workspaceIdentity: "replacement-inode" }, request)
                .pipe(Effect.result))._tag,
            ).toBe("Failure");
          else expect((yield* replacement.start(f.owner, request)).jobId).toBe(oldId);
          expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("x");
          expect(
            yield* f.fs.readDirectory(`${f.dir}/state/userdata/workspace-command-jobs`),
          ).toEqual([oldId]);
        }),
      ),
  );
  it.live("ignores abandoned pre-request directories but refuses a known incomplete claim", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const crypto = yield* Crypto.Crypto;
        const root = `${f.dir}/state/userdata/workspace-command-jobs`;
        yield* f.fs.makeDirectory(`${root}/${"a".repeat(64)}`);
        const request = {
          command: "printf x >> starts",
          requestId: "after-empty-claim",
          yieldMs: 10000,
        };
        const result = yield* f.jobs.start(f.owner, request);
        expect(result.status).toBe("completed");
        const pendingKey = "incomplete-stable-claim";
        const pendingId = Hex.encode(
          yield* crypto.digest(
            "SHA-256",
            new TextEncoder().encode(
              yield* encodeJson([f.owner.environmentId, f.owner.threadId, pendingKey]),
            ),
          ),
        );
        yield* f.fs.makeDirectory(`${root}/${pendingId}`);
        expect(
          (yield* f.jobs.start(f.owner, { ...request, requestId: pendingKey }).pipe(Effect.result))
            ._tag,
        ).toBe("Failure");
        expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("x");
      }),
    ),
  );
  it.live("refuses a new launch when an admitted legacy receipt cannot be decoded", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const corrupt = `${f.dir}/state/userdata/workspace-command-jobs/${"b".repeat(64)}`;
        yield* f.fs.makeDirectory(corrupt);
        yield* f.fs.writeFileString(`${corrupt}/request.json`, "invalid");
        yield* f.fs.writeFileString(`${corrupt}/status.json`, "execution-was-admitted");
        const result = yield* f.jobs
          .start(f.owner, {
            command: "printf unexpected >> starts",
            requestId: "possibly-executed",
            yieldMs: 10000,
          })
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure.message).toContain(
            "unreadable request journal after execution was admitted",
          );
        expect(yield* f.fs.exists(`${f.dir}/starts`)).toBe(false);
      }),
    ),
  );
  it.live("does not combine another environment or thread's explicit request key", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const request = {
          command: "printf x >> starts",
          requestId: "shared-client-key",
          yieldMs: 10000,
        };
        const initial = yield* f.jobs.start(f.owner, request);
        const otherThread = yield* f.jobs.start(
          { ...f.owner, threadId: "another-thread" },
          request,
        );
        const otherEnvironment = yield* f.jobs.start(
          { ...f.owner, environmentId: "another-environment" },
          request,
        );
        expect(new Set([initial.jobId, otherThread.jobId, otherEnvironment.jobId]).size).toBe(3);
        expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("xxx");
      }),
    ),
  );
  it.live("preserves stale outcomes as uncertain and never restarts the command", () =>
    run(
      Effect.gen(function* () {
        const f = yield* fixture;
        const request = {
          command: "printf once >> starts",
          requestId: "uncertain-start",
          yieldMs: 10000,
        };
        const result = yield* f.jobs.start(f.owner, request);
        const statusPath = `${f.jobDir(result.jobId)}/status.json`;
        const stale = yield* encodeStaleStatus({
          status: "running",
          exitCode: null,
          timedOut: false,
          updatedAt: 0,
        });
        yield* f.fs.writeFileString(statusPath, stale);
        expect(yield* f.jobs.start(f.owner, request)).toMatchObject({
          jobId: result.jobId,
          status: "interrupted",
          exitCode: null,
        });
        yield* f.jobs.cancel(f.owner, result.jobId);
        expect(yield* f.fs.exists(`${f.jobDir(result.jobId)}/cancel`)).toBe(true);
        expect(yield* f.fs.readFileString(`${f.dir}/starts`)).toBe("once");
      }),
    ),
  );
});
