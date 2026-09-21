# Fabric workspace

This mount exposes Fabric workspaces directly at its root. Load the
`fabric-notebook-workflow` skill before any Fabric Notebook task.

- Edit existing Notebooks by saving `{Notebook Name}.ipynb` in place.
- Use `.fabric.json` for the containing object’s immutable ID, type, and
  related identity fields. It is read-only metadata, not a Fabric definition.
- Do not infer remote IDs from display names or collision suffixes.
- Ask before executing code, starting Spark, installing libraries, deleting
  remote data, or accessing another workspace.
- Keep credentials, agent state, and temporary files outside the mount.
