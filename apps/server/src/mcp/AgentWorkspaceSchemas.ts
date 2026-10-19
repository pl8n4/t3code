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
