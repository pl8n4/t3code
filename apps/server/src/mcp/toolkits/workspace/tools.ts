import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Workspace from "../../AgentWorkspaceService.ts";
import { WorkspaceInfo } from "../../AgentWorkspaceSchemas.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Invocation from "../../McpInvocationContext.ts";

const shared = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    Workspace.AgentWorkspaceService,
    Invocation.McpInvocationContext,
    Threads.ThreadManagementService,
  ],
  success: Schema.Unknown,
};
export const WorkspaceToolkit = Toolkit.make(
  Tool.make("t3_workspace_info", {
    ...shared,
    parameters: Tool.EmptyParams,
    success: WorkspaceInfo,
    description:
      "Identify this thread's actual local project, repository, branch and worktree. The cloud container's /home/user directory is unrelated to this workspace.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_pull_requests", {
    ...shared,
    parameters: Workspace.WorkspacePullRequests,
    description:
      "List recent pull requests for this thread's project through T3's existing source-control account. Defaults to all states, including merged PRs. Query searches titles and descriptions; use filters.labels for precise topic labels. If truncated is true, pass nextCursors as cursors to read the next page. Search enough candidates and filter topic matches before reporting a requested number; do not pad a documentation list with unrelated bugfix PRs. No cloud GitHub login or repo clone is needed.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_pull_request_read", {
    ...shared,
    parameters: Workspace.WorkspacePullRequestRead,
    description:
      "Read a pull request's description, change counts and status using the repository and number from t3_workspace_pull_requests. For exact changed paths or patches, use t3_workspace_exec with gh pr view --json files or gh pr diff. Uses the existing T3 source-control account on the user's machine.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_list", {
    ...shared,
    parameters: Workspace.WorkspaceList,
    description:
      "List the actual local files of this T3 thread. Use this instead of cloud Glob or Bash to inspect the project.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_read", {
    ...shared,
    parameters: Workspace.WorkspaceRead,
    description:
      "Read a text file in this T3 thread's actual local project. Returns a version for safe writes and nextOffset for paging. Paths are workspace-relative.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_write", {
    ...shared,
    parameters: Workspace.WorkspaceWrite,
    description:
      "Write a text file directly in this T3 thread's local project. Pass the version from t3_workspace_read to avoid overwriting intervening edits; use null only when creating a new file. Changes appear immediately in T3's Changes panel.",
  }).annotate(Tool.Destructive, true),
  Tool.make("t3_workspace_exec", {
    ...shared,
    parameters: Workspace.WorkspaceExec,
    description:
      "Run a shell command in this T3 thread's actual local workspace, with Full access. Use for rg, git, tests, and patching project files. Cloud Bash executes on a different machine. Commands have no deadline unless timeoutMs is supplied. Short commands return output immediately; longer commands return a durable jobId. Use t3_workspace_job_read to wait for status and page output, and t3_workspace_job_cancel to stop a job. requestId makes retries return the same job instead of repeating the command. Jobs survive cloud and server reconnects.",
  }).annotate(Tool.Destructive, true),
  Tool.make("t3_workspace_job_read", {
    ...shared,
    parameters: Workspace.WorkspaceJobRead,
    description:
      "Read a command job's status and bounded stdout/stderr. Offsets are bytes; pass nextStdoutOffset/nextStderrOffset to read subsequent output. waitMs waits up to 30 seconds for completion. An interrupted job has uncertain outcome: inspect its logs and workspace before starting replacement work. Requires the same thread/workspace and Full access.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_job_list", {
    ...shared,
    parameters: Workspace.WorkspaceJobList,
    description:
      "List retained command jobs for this exact thread and current workspace, newest first. Use after reconnecting or losing a command-start response. Requires Full access.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_workspace_job_cancel", {
    ...shared,
    parameters: Workspace.WorkspaceJobId,
    description:
      "Request cancellation of a running command job and its process group. Read the job again to confirm its final status. Cancellation does not undo changes already made. Requires the same thread/workspace and Full access.",
  }).annotate(Tool.Destructive, true),
);
