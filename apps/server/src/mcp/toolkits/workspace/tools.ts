import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as Workspace from "../../AgentWorkspaceService.ts";
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
      "Run a shell command in this T3 thread's actual local workspace, with Full access. Use for rg, git, tests, and patching project files. Cloud Bash executes on a different machine. Output is bounded; redirect large output to a workspace file and read it in chunks. Commands time out after 60 seconds.",
  }).annotate(Tool.Destructive, true),
);
