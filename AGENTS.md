# FerrisSearch Agent Entry Point

This file is the model-neutral entry point for automated coding agents.

## Required Reading Order

1. Read [`.github/copilot-instructions.md`](.github/copilot-instructions.md)
   for repository-wide mission, authority, architecture, invariants, workflow,
   and validation.
2. Read only the path-specific files in
   [`.github/instructions/`](.github/instructions/) that match the files you
   will change. The global instruction file contains the registry.
3. Read [`docs/architecture-roadmap.md`](docs/architecture-roadmap.md) before
   architecture, storage lifecycle, distributed correctness, or roadmap work.
4. Read [`docs/next-50-tasks.md`](docs/next-50-tasks.md) before selecting or
   claiming priority for roadmap work.
5. Read [`docs/ai-agent-guide.md`](docs/ai-agent-guide.md) for context-loading,
   planning, handoff, and instruction-maintenance practices.
6. Treat current source and tests as implemented truth. Treat roadmap and
   backlog documents as intended future state.

Do not load every instruction file preemptively. FerrisSearch is large enough
that unrelated detail lowers accuracy and increases the chance of following a
stale or inapplicable rule.

## Work Contract

- Inspect before editing; trace the complete call path and locate prior art.
- Protect the global invariants in `.github/copilot-instructions.md`.
- Keep one session/change focused on one coherent, reviewable outcome.
- For roadmap work, identify the roadmap gate, prerequisite, and success
  evidence before implementation.
- Add result-level tests, not only plan/metadata assertions.
- Distributed behavior needs distributed or transport-level coverage.
- Run focused checks first, then the canonical checks appropriate to impact.
- Update public docs only with source-backed current behavior, explicitly
  separated from experimental and planned behavior.
- Leave a concise handoff containing changed files, decisions, validation,
  limitations, and the next dependency.

## High-Risk Areas

Pause and load the owning instructions before changing:

- Raft commands or `ClusterState`
- WAL sequence ownership or recovery
- shard UUID paths and startup cleanup
- manifest publication or object deletion
- transport serialization
- SQL planner/fast-field/grouped execution
- security metadata or protected-index visibility
- async/blocking scheduling and resource limits

If instructions conflict, follow source/tests for current behavior and the
repository-wide authority order for intended changes. Correct the stale
instruction in the same change when it directly affects the work.
