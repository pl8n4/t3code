import * as Effect from "effect/Effect";
import * as Workspace from "../../AgentWorkspaceService.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import * as Access from "../../McpToolAccess.ts";
import { WorkspaceToolkit } from "./tools.ts";

export const layer = Access.toLayer(
  WorkspaceToolkit,
  Effect.gen(function* () {
    const workspace = yield* Workspace.AgentWorkspaceService;
    return {
      t3_workspace_info: Access.readsAsCaller(() =>
        Effect.gen(function* () {
          return yield* workspace.info(yield* Invocation.McpInvocationContext);
        }),
      ),
      t3_workspace_pull_requests: Access.readsAsCaller((input) =>
        Effect.gen(function* () {
          return yield* workspace.pullRequests(yield* Invocation.McpInvocationContext, input);
        }),
      ),
      t3_workspace_pull_request_read: Access.readsAsCaller((input) =>
        Effect.gen(function* () {
          return yield* workspace.pullRequestRead(yield* Invocation.McpInvocationContext, input);
        }),
      ),
      t3_workspace_list: Access.readsAsCaller((input) =>
        Effect.gen(function* () {
          const scope = yield* Invocation.McpInvocationContext;
          return yield* workspace.list(scope, input);
        }),
      ),
      t3_workspace_read: Access.readsAsCaller((input) =>
        Effect.gen(function* () {
          const scope = yield* Invocation.McpInvocationContext;
          return yield* workspace.read(scope, input);
        }),
      ),
      t3_workspace_write: Access.actsAsCaller((input) =>
        Effect.gen(function* () {
          const scope = yield* Invocation.McpInvocationContext;
          return yield* workspace.write(scope, input);
        }),
      ),
      t3_workspace_job_read: Access.readsAsCaller((input) =>
        Effect.gen(function* () {
          return yield* workspace.jobRead(yield* Invocation.McpInvocationContext, input);
        }),
      ),
      t3_workspace_job_list: Access.readsAsCaller((input) =>
        Effect.gen(function* () {
          return yield* workspace.jobList(yield* Invocation.McpInvocationContext, input);
        }),
      ),
      t3_workspace_job_cancel: Access.actsAsCaller((input) =>
        Effect.gen(function* () {
          return yield* workspace.jobCancel(yield* Invocation.McpInvocationContext, input);
        }),
      ),
      t3_workspace_exec: Access.actsAsCaller((input) =>
        Effect.gen(function* () {
          const scope = yield* Invocation.McpInvocationContext;
          return yield* workspace.exec(scope, input);
        }),
      ),
    };
  }),
);
