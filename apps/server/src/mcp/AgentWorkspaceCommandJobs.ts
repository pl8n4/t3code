import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { CommandResolutionCache, resolveCommandPath } from "@t3tools/shared/shell";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Hex from "effect/encoding/Hex";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as Config from "../config.ts";
import {
  WorkspaceCommandJob,
  WorkspaceCommandJobState,
  type WorkspaceExec,
  type WorkspaceJobRead,
} from "./AgentWorkspaceSchemas.ts";
import { unavailable } from "./threadAccess.ts";

const Owner = Schema.Struct({
  environmentId: Schema.String,
  threadId: Schema.String,
  projectId: Schema.String,
  cwd: Schema.String,
  workspaceIdentity: Schema.String,
});
const RequestKey = Schema.fromJsonString(Schema.Tuple([Owner, Schema.String]));
const StableRequestKey = Schema.fromJsonString(
  Schema.Tuple([Schema.String, Schema.String, Schema.String]),
);
const Request = Schema.Struct({
  owner: Owner,
  requestId: Schema.optionalKey(Schema.String),
  command: Schema.String,
  timeoutMs: Schema.NullOr(Schema.Number),
  createdAt: Schema.Number,
});
const Status = Schema.Struct({
  status: WorkspaceCommandJobState,
  exitCode: Schema.NullOr(Schema.Number),
  timedOut: Schema.Boolean,
  updatedAt: Schema.Number,
});

const encodeKey = Schema.encodeEffect(RequestKey);
const encodeStableKey = Schema.encodeEffect(StableRequestKey);
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Request));
const encodeStatus = Schema.encodeEffect(Schema.fromJsonString(Status));
const decodeRequest = Schema.decodeUnknownEffect(Schema.fromJsonString(Request));
const decodeStatus = Schema.decodeUnknownEffect(Schema.fromJsonString(Status));

// The detached supervisor owns the process group and stays alive through cancellation.
// Its group id cannot be reused while it is signalling descendants, even after the shell exits.
const supervisor = String.raw`
const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");
const dir = process.argv[1];
const request = JSON.parse(fs.readFileSync(path.join(dir, "request.json"), "utf8"));
let state = {status:"starting",exitCode:null,timedOut:false,updatedAt:Date.now()};
const save = () => {
  state.updatedAt = Date.now();
  const file = path.join(dir,"status.json");
  fs.writeFileSync(file+".tmp",JSON.stringify(state),{mode:0o600});
  fs.renameSync(file+".tmp",file);
};
let cancelling = false;
let terminal = false;
let heartbeat;
let deadline;
process.on("SIGTERM", () => {});
process.on("uncaughtException", () => {
  try { state.status = "interrupted"; save(); } catch {}
  process.kill(-process.pid,"SIGKILL");
});
const cancel = (timedOut) => {
  if (cancelling || terminal) return;
  cancelling = true;
  state.timedOut = timedOut;
  try { process.kill(-process.pid,"SIGTERM"); } catch {}
  setTimeout(() => {
    clearInterval(heartbeat);
    clearTimeout(deadline);
    state.status = timedOut ? "timed_out" : "cancelled";
    save();
    process.kill(-process.pid,"SIGKILL");
  },2000);
};
const finish = (status,code) => {
  if (terminal || cancelling) return;
  terminal = true;
  clearInterval(heartbeat);
  clearTimeout(deadline);
  fs.closeSync(stdout);
  fs.closeSync(stderr);
  state.status = status;
  state.exitCode = code;
  save();
};
const stdout = fs.openSync(path.join(dir,"stdout.log"),"a",0o600);
const stderr = fs.openSync(path.join(dir,"stderr.log"),"a",0o600);
let child;
try {
  child = spawn("sh",["-lc",request.command],{
    cwd:request.owner.cwd,detached:false,stdio:["ignore","pipe","pipe"],env:process.env
  });
  child.stdout.on("data",(chunk) => fs.writeSync(stdout,chunk));
  child.stderr.on("data",(chunk) => fs.writeSync(stderr,chunk));
  child.once("error",() => finish("failed",null));
  child.once("close",(code,signal) => finish(signal === null ? "completed" : "interrupted",code));
  state.status = "running";
  save();
  heartbeat = setInterval(() => {
    if (fs.existsSync(path.join(dir,"cancel"))) cancel(false);
    if (!terminal) save();
  },250);
  if (request.timeoutMs !== null) deadline = setTimeout(() => cancel(true),request.timeoutMs);
} catch { finish("failed",null); }
`;

const failure = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });
const terminal = (status: typeof WorkspaceCommandJobState.Type) =>
  status !== "starting" && status !== "running";

export const makeCommandJobs = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const config = yield* Config.ServerConfig;
  const root = path.join(config.stateDir, "workspace-command-jobs");
  const claims = yield* Semaphore.make(1);
  yield* fs
    .makeDirectory(root, { recursive: true, mode: 0o700 })
    .pipe(Effect.mapError(unavailable));
  const digest = (value: string) =>
    crypto.digest("SHA-256", new TextEncoder().encode(value)).pipe(Effect.map(Hex.encode));
  const directory = (jobId: string) =>
    /^[a-f0-9]{64}$/.test(jobId)
      ? Effect.succeed(path.join(root, jobId))
      : Effect.fail(
          failure("Use the jobId returned by t3_workspace_exec or t3_workspace_job_list."),
        );
  const readRequest = (dir: string) =>
    fs
      .readFileString(path.join(dir, "request.json"))
      .pipe(Effect.flatMap(decodeRequest), Effect.mapError(unavailable));
  const legacyJob = Effect.fnUntraced(function* (owner: typeof Owner.Type, requestId: string) {
    const names = yield* fs.readDirectory(root).pipe(Effect.mapError(unavailable));
    let found: string | undefined;
    for (const name of names) {
      if (!/^[a-f0-9]{64}$/.test(name)) continue;
      const dir = path.join(root, name);
      const decoded = yield* readRequest(dir).pipe(Effect.result);
      if (decoded._tag === "Failure") {
        const admitted = yield* Effect.forEach(
          ["status.json", "stdout.log", "stderr.log"],
          (file) => fs.exists(path.join(dir, file)).pipe(Effect.mapError(unavailable)),
        );
        if (admitted.some(Boolean))
          return yield* failure(
            `Retained command job ${name} has an unreadable request journal after execution was admitted. Recover its request.json before starting new work; its previous thread and requestId cannot be verified.`,
          );
        // Both supervisor launch and status/output creation follow the request write.
        // An abandoned pre-request directory cannot contain an executed command.
        continue;
      }
      const request = decoded.success;
      if (
        request.owner.environmentId !== owner.environmentId ||
        request.owner.threadId !== owner.threadId
      )
        continue;
      const matches =
        request.requestId === undefined
          ? (yield* digest(
              yield* encodeKey([request.owner, requestId]).pipe(Effect.mapError(unavailable)),
            ).pipe(Effect.mapError(unavailable))) === name
          : request.requestId === requestId;
      if (!matches) continue;
      if (found !== undefined)
        return yield* failure(
          "Several retained jobs match this requestId. Inspect the previous jobs before starting replacement work.",
        );
      found = name;
    }
    return found;
  });
  const authorize = Effect.fnUntraced(function* (owner: typeof Owner.Type, jobId: string) {
    const dir = yield* directory(jobId);
    const request = yield* readRequest(dir);
    if (
      request.owner.environmentId !== owner.environmentId ||
      request.owner.threadId !== owner.threadId ||
      request.owner.projectId !== owner.projectId ||
      request.owner.cwd !== owner.cwd ||
      request.owner.workspaceIdentity !== owner.workspaceIdentity
    )
      return yield* failure("This job does not belong to the calling thread's current workspace.");
    return { dir, request };
  });
  const readStatus = Effect.fnUntraced(function* (dir: string) {
    const status = yield* fs
      .readFileString(path.join(dir, "status.json"))
      .pipe(Effect.flatMap(decodeStatus), Effect.mapError(unavailable));
    const now = yield* Clock.currentTimeMillis;
    // A stale journal does not prove that a process made no changes. Never start it again.
    const mayRun = !terminal(status.status);
    return {
      ...status,
      status: mayRun && now - status.updatedAt > 15_000 ? ("interrupted" as const) : status.status,
      mayRun,
    };
  });
  const readOutput = Effect.fnUntraced(function* (
    filePath: string,
    offset: number,
    maxBytes: number,
    holdIncomplete: boolean,
  ) {
    if (!(yield* fs.exists(filePath).pipe(Effect.mapError(unavailable))))
      return { text: "", nextOffset: offset, more: false };
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fs.open(filePath, { flag: "r" });
        const stat = yield* file.stat;
        yield* file.seek(BigInt(offset), "start");
        const buffer = new Uint8Array(maxBytes + 3);
        const count = yield* file.read(buffer);
        let end = Math.min(maxBytes, count);
        while (end < count && (buffer[end]! & 0xc0) === 0x80) end++;
        if (holdIncomplete && end === count && end > 0) {
          let start = end - 1;
          while (start > 0 && (buffer[start]! & 0xc0) === 0x80) start--;
          const lead = buffer[start]!;
          const width =
            lead >= 0xf0 && lead <= 0xf4
              ? 4
              : lead >= 0xe0 && lead <= 0xef
                ? 3
                : lead >= 0xc2 && lead <= 0xdf
                  ? 2
                  : 1;
          if (end - start < width) end = start;
        }
        return {
          text: new TextDecoder().decode(buffer.subarray(0, end)),
          nextOffset: offset + end,
          more: BigInt(offset + end) < stat.size,
        };
      }),
    ).pipe(Effect.mapError(unavailable));
  });
  const read = Effect.fn("AgentWorkspaceService.jobRead")(function* (
    owner: typeof Owner.Type,
    input: typeof WorkspaceJobRead.Type,
  ) {
    const { dir, request } = yield* authorize(owner, input.jobId);
    let status = yield* readStatus(dir);
    const until = (yield* Clock.currentTimeMillis) + (input.waitMs ?? 0);
    while (!terminal(status.status) && (yield* Clock.currentTimeMillis) < until) {
      yield* Effect.sleep(100);
      status = yield* readStatus(dir);
    }
    const stdoutOffset = input.stdoutOffset ?? 0;
    const stderrOffset = input.stderrOffset ?? 0;
    const maxBytes = input.maxBytes ?? 16000;
    const [stdout, stderr] = yield* Effect.all([
      readOutput(path.join(dir, "stdout.log"), stdoutOffset, maxBytes, status.mayRun),
      readOutput(path.join(dir, "stderr.log"), stderrOffset, maxBytes, status.mayRun),
    ]);
    return {
      jobId: input.jobId,
      command: request.command,
      status: status.status,
      stdout: stdout.text,
      stderr: stderr.text,
      exitCode: status.exitCode,
      timedOut: status.timedOut,
      truncated: stdout.more || stderr.more,
      stdoutOffset,
      stderrOffset,
      nextStdoutOffset: stdout.nextOffset,
      nextStderrOffset: stderr.nextOffset,
      createdAt: request.createdAt,
      updatedAt: status.updatedAt,
    } satisfies typeof WorkspaceCommandJob.Type;
  });
  return {
    start: Effect.fn("AgentWorkspaceService.execJob")(function* (
      owner: typeof Owner.Type,
      input: typeof WorkspaceExec.Type,
    ) {
      if ((yield* HostProcess.Platform) === "win32")
        return yield* failure("Workspace command jobs require a Unix shell.");
      const executable = (yield* HostProcess.IsExecutable)
        ? yield* resolveCommandPath("node", { env: yield* HostProcess.Environment }).pipe(
            Effect.provideService(CommandResolutionCache, new Map()),
            Effect.provideService(FileSystem.FileSystem, fs),
            Effect.provideService(Path.Path, path),
            Effect.mapError(unavailable),
          )
        : yield* HostProcess.ExecutablePath;
      if (executable === null)
        return yield* failure("Workspace command jobs require Node.js on PATH.");
      const requestId =
        input.requestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.mapError(unavailable)));
      const jobId = yield* claims.withPermits(1)(
        Effect.gen(function* () {
          const stableId = yield* digest(
            yield* encodeStableKey([owner.environmentId, owner.threadId, requestId]).pipe(
              Effect.mapError(unavailable),
            ),
          ).pipe(Effect.mapError(unavailable));
          const stableDir = yield* directory(stableId);
          // Workspace binding is authorization, not request identity: changing a checkout
          // must refuse an uncertain retry rather than turn it into a new command.
          const previousId = (yield* fs.exists(stableDir).pipe(Effect.mapError(unavailable)))
            ? stableId
            : yield* legacyJob(owner, requestId);
          if (previousId !== undefined) {
            const { request } = yield* authorize(owner, previousId);
            if (
              request.command !== input.command ||
              request.timeoutMs !== (input.timeoutMs ?? null)
            )
              return yield* failure(
                "This requestId already belongs to a different command or deadline.",
              );
            return previousId;
          }
          const created = yield* fs.makeDirectory(stableDir, { mode: 0o700 }).pipe(Effect.result);
          if (created._tag === "Failure") {
            if (!(yield* fs.exists(stableDir).pipe(Effect.mapError(unavailable))))
              return yield* unavailable();
            const { request } = yield* authorize(owner, stableId);
            if (
              request.command !== input.command ||
              request.timeoutMs !== (input.timeoutMs ?? null)
            )
              return yield* failure(
                "This requestId already belongs to a different command or deadline.",
              );
            return stableId;
          }
          yield* Effect.gen(function* () {
            const createdAt = yield* Clock.currentTimeMillis;
            yield* fs.writeFileString(
              path.join(stableDir, "request.json"),
              yield* encodeRequest({
                owner,
                requestId,
                command: input.command,
                timeoutMs: input.timeoutMs ?? null,
                createdAt,
              }),
              { mode: 0o600 },
            );
            yield* fs.writeFileString(
              path.join(stableDir, "status.json"),
              yield* encodeStatus({
                status: "starting",
                exitCode: null,
                timedOut: false,
                updatedAt: createdAt,
              }),
              { mode: 0o600 },
            );
            // Unref also disables the spawner's scope cleanup; the supervisor is server-independent.
            const _reref = yield* spawner
              .spawn(
                ChildProcess.make(executable, ["-e", supervisor, stableDir], {
                  cwd: owner.cwd,
                  detached: true,
                  stdin: "ignore",
                  stdout: "ignore",
                  stderr: "ignore",
                }),
              )
              .pipe(
                Effect.flatMap((handle) => handle.unref),
                Effect.scoped,
              );
          }).pipe(Effect.uninterruptible, Effect.mapError(unavailable));
          return stableId;
        }),
      );
      return yield* read(owner, { jobId, waitMs: input.yieldMs ?? 1000 });
    }),
    read,
    cancel: Effect.fn("AgentWorkspaceService.jobCancel")(function* (
      owner: typeof Owner.Type,
      jobId: string,
    ) {
      const { dir } = yield* authorize(owner, jobId);
      const status = yield* readStatus(dir);
      if (status.mayRun || status.status === "interrupted")
        yield* fs
          .writeFileString(path.join(dir, "cancel"), "", { mode: 0o600 })
          .pipe(Effect.mapError(unavailable));
      return yield* read(owner, { jobId, waitMs: 0 });
    }),
    list: Effect.fn("AgentWorkspaceService.jobList")(function* (
      owner: typeof Owner.Type,
      limit: number,
    ) {
      const names = yield* fs.readDirectory(root).pipe(Effect.mapError(unavailable));
      const jobs = yield* Effect.forEach(
        names,
        (jobId) => read(owner, { jobId, maxBytes: 1 }).pipe(Effect.option),
        { concurrency: 8 },
      );
      return jobs
        .flatMap((job) =>
          job._tag === "Some"
            ? [
                {
                  jobId: job.value.jobId,
                  command: job.value.command.slice(0, 240),
                  status: job.value.status,
                  exitCode: job.value.exitCode,
                  timedOut: job.value.timedOut,
                  createdAt: job.value.createdAt,
                  updatedAt: job.value.updatedAt,
                },
              ]
            : [],
        )
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, limit);
    }),
  };
});
