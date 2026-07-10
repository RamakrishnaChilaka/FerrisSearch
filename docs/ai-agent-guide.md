# AI Agent Guide

> **Status:** Canonical guide for how GPT, Claude, Copilot, Codex, and other
> coding agents should acquire context and execute work in FerrisSearch.
>
> **Implementation authority:** current source and tests.
>
> **Strategic authority:** [`architecture-roadmap.md`](architecture-roadmap.md).

## 1. Why FerrisSearch Uses Progressive Disclosure

FerrisSearch spans Raft, Tantivy, Arrow/DataFusion, vector indexing, gRPC,
object storage, security, lifecycle recovery, SQL, and public APIs. A single
instruction dump large enough to explain every subsystem has two predictable
failure modes:

1. relevant constraints are buried in unrelated detail; and
2. copied implementation snapshots become stale while still being loaded for
   every task.

The repository therefore separates context into four layers:

| Layer | Purpose | Stability |
|---|---|---|
| `.github/copilot-instructions.md` | Mission, authority, architecture map, global invariants, workflow | High |
| `.github/instructions/*.instructions.md` | Path-scoped subsystem contracts and traps | Medium |
| `AGENTS.md` / `CLAUDE.md` | Thin model-specific discovery adapters | High |
| Source, tests, roadmap, and focused docs | Exact current behavior and intended future state | Varies by owner |

Agents should load the smallest context set that can safely govern the task.
This is not a request to skip investigation. It is a request to investigate
the right code and guidance instead of consuming every historical fact.

## 2. Authority And Conflict Resolution

Use this order:

1. **Source and tests** answer "what exists now?"
2. **Architecture roadmap** answers "where should the system go?"
3. **Scoped instructions** answer "what invariants and conventions govern this
   area?"
4. **README and supporting docs** explain the system to users and may lag.

Examples:

- A roadmap describes fenced multi-writer publication, while source has a
  process-local manifest mutex. The current behavior is single-process
  serialization; fencing is future work.
- A historical note calls `remote_store` future-only, while current source has
  manifest-backed reads. Source wins and the note should be corrected.
- A test encodes behavior that contradicts a newly approved roadmap invariant.
  Do not silently follow either. Identify whether the task intentionally
  changes behavior, then update implementation and tests together.

Never use the roadmap as evidence that a feature shipped. Never use old source
behavior as a reason to ignore an explicit future architecture decision.

## 3. Model Entry Points

| Agent family | Entry point | Resolution |
|---|---|---|
| GitHub Copilot | `.github/copilot-instructions.md` | Global file plus matching `.github/instructions/*.instructions.md` rules |
| OpenAI Codex and AGENTS-aware tools | `AGENTS.md` | Directs the agent to the global contract and scoped registry |
| Claude Code | `CLAUDE.md` | Imports `AGENTS.md`, preserving one model-neutral contract |
| Other agents | `AGENTS.md` | Use the same loading algorithm manually |

Do not copy the global contract into each model file. The adapters should
remain thin so a rule cannot be corrected in one place and remain stale in
another.

## 4. Context-Loading Algorithm

### Step 1: Classify The Task

Choose the closest class:

- **Tactical fix:** one bounded current-behavior defect.
- **Subsystem feature:** one area with several code/test surfaces.
- **Cross-cutting change:** protocol, lifecycle, security, resource, or
  compatibility behavior spanning subsystems.
- **Roadmap work:** advances a named gate or research hypothesis.
- **Documentation/evidence:** changes public claims, benchmarks, instructions,
  roadmap, or runbooks.

This classification determines how much planning and context are justified.

### Step 2: Build A Minimal Context Pack

Always load:

- `.github/copilot-instructions.md`;
- the relevant scoped instruction file(s);
- implementation entry points and direct callers/callees;
- nearest unit and integration tests.

Add when relevant:

- `architecture-roadmap.md` for cross-cutting or roadmap work;
- `next-50-tasks.md` for priority/dependency context;
- protocol definitions and both encoder/decoder sides for wire changes;
- persistence format and restart tests for durable-state changes;
- documentation instructions for public claims.

Do not load every scoped subsystem file "just in case."

### Step 3: Verify Volatile Facts

Before relying on a copied number or shape, inspect source:

- struct fields and optionality;
- dependency and package versions;
- endpoints and response fields;
- config defaults;
- test counts;
- benchmark results;
- supported engine behavior.

Instructions should mostly contain invariants and traps, but source remains the
authority for volatile detail.

## 5. Execution Protocol For Future Sessions

### 5.1 Orient

1. Read the task and authority chain.
2. Inspect repository status without reverting unrelated work.
3. Find the implementation path with code search.
4. Read neighboring tests and scoped instructions.
5. State the current behavior and exact gap in working notes.

### 5.2 Plan At The Right Size

Use an explicit plan when work spans phases or files. The plan should include:

- user-visible or system-level outcome;
- invariants at risk;
- source surfaces;
- test/evidence surfaces;
- dependencies and non-goals;
- roadmap gate/task ID when applicable.

Avoid plans that merely list file edits. A useful plan explains why those edits
form a correct end-to-end change.

### 5.3 Implement Vertically

Prefer one thin complete slice over disconnected scaffolding. Depending on the
task, a complete slice can include:

- domain model and validation;
- persistence/serde compatibility;
- transport encode/decode and limits;
- coordinator routing;
- engine or storage behavior;
- errors and observability;
- result-level unit/integration tests;
- relevant user/operator documentation.

Do not add a new abstraction only in anticipation of later roadmap work unless
the current slice exercises and validates it.

### 5.4 Validate Evidence, Not Appearance

Validation should answer the requirement directly:

- Correct output values, not only a planner flag.
- Restarted data, not only successful serialization.
- Follower-forwarded mutation, not only leader-local mutation.
- Independent publishers, not only tasks sharing one mutex.
- Cache byte limits under concurrency, not only cache entry counts.
- Cancellation that stops child work, not only a timed-out HTTP future.
- Compatibility request/response fixtures, not only similar endpoint names.

Run focused checks during iteration. Before completion, run the widest existing
checks justified by the change.

### 5.5 Hand Off For The Next Session

A durable handoff contains:

- outcome and behavior change;
- files and symbols changed;
- architecture decisions or assumptions;
- tests and observed results;
- known limits or skipped external evidence;
- stable roadmap/task IDs advanced;
- the next dependency, if any.

Do not use README or a new repository Markdown file as an ad hoc session log.

## 6. Prompt Design For Delegated Work

A self-contained task for another agent should provide:

1. **Goal:** one measurable outcome.
2. **Current evidence:** paths and symbols that establish the gap.
3. **Constraints:** invariants and prohibited shortcuts.
4. **Scope:** expected surfaces and explicit non-goals.
5. **Definition of done:** behavior and tests, not file count.
6. **Validation:** exact commands or required live/failure evidence.
7. **Handoff:** what the agent must report.

Example skeleton:

```text
Implement <outcome> to advance roadmap task FS-###.

Current behavior:
- <path:symbol and observed limitation>

Protect:
- <invariant>

Scope:
- <surfaces>

Do not:
- <tempting but incorrect shortcut>

Done when:
- <result-level acceptance criteria>
- <failure/restart/distributed criteria>

Validate with:
- <focused commands>
```

Avoid vague prompts such as "improve remote store" or "make this production
ready." They invite broad changes without a falsifiable completion condition.

## 7. Maintaining The Instruction System

### Global File

Keep `.github/copilot-instructions.md` broadly applicable and roughly within
two printed pages / about 200 lines. It should contain stable rules, not:

- complete structs or endpoint catalogs;
- exact test counts;
- long subsystem algorithms;
- duplicate examples already owned by scoped files; or
- unimplemented roadmap detail.

### Scoped Files

Every `.github/instructions/*.instructions.md` file must start with YAML:

```yaml
---
description: "When this rule is relevant."
applyTo: "src/example/**,tests/example_*"
---
```

Choose the narrowest useful `applyTo`. Overlap is appropriate when two
contracts genuinely govern a file, such as hybrid SQL and API behavior in
`src/api/search/mod.rs`.

Each scoped file should emphasize:

- architecture boundary;
- correctness invariants;
- current intentional limitations;
- common failure patterns;
- required tests.

Avoid exhaustive snapshots of all methods unless a type shape itself is a
critical invariant.

### Drift Review

When a change invalidates an instruction, update the owning scoped file in the
same change. Periodically audit:

```bash
find .github/instructions -name '*.instructions.md' -print
rg -L '^---$' .github/instructions/*.instructions.md
rg --glob '!docs/ai-agent-guide.md' \
  'Option<Arc<RaftInstance>>|only operational engine|OpenSearch-compatible|total tests' \
  .github README.md docs
```

Also verify:

- every registry entry points to an existing file;
- no obsolete non-`.instructions.md` rule remains;
- all repository-relative links resolve;
- source comments do not contradict current engine maturity;
- `AGENTS.md` and `CLAUDE.md` remain thin adapters.

## 8. Large-Codebase Practices Adopted Here

FerrisSearch follows these evidence-backed practices:

- **Small global context:** broadly applicable rules stay concise.
- **Path-scoped detail:** subsystem guidance is loaded only for matching work.
- **Hierarchical/model-neutral discovery:** `AGENTS.md` bridges tools without
  duplicating the contract.
- **Explicit authority:** current implementation and intended direction cannot
  be accidentally conflated.
- **Concrete commands and symbols:** agents can validate rather than guess.
- **Stable invariants over snapshots:** volatile details are checked in source.
- **Task-sized sessions:** one coherent outcome reduces partial cross-cutting
  changes.
- **Result-level evidence:** tests prove semantics, restart, distribution, and
  failure handling where those properties matter.

## 9. Official Guidance Used

This structure is based on official documentation available at the time of the
instruction overhaul:

- GitHub, [Adding repository custom instructions for GitHub Copilot](https://docs.github.com/en/copilot/how-tos/copilot-on-github/customize-copilot/add-custom-instructions/add-repository-instructions)
  - repository-wide instructions, path-specific `applyTo`, `AGENTS.md`, and
    model-specific instruction files.
- GitHub, [How to write effective custom instructions for GitHub Copilot](https://docs.github.com/en/copilot/tutorials/customization-library/custom-instructions/your-first-custom-instructions)
  - concise, broadly applicable repository context and concrete guidance.
- Anthropic, [Manage Claude's memory](https://code.claude.com/docs/en/memory)
  - concise `CLAUDE.md`, imports, path-scoped rules, and progressive disclosure.
- OpenAI, [Custom instructions with AGENTS.md](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
  - hierarchical `AGENTS.md` discovery and bounded combined instruction size.

These sources guide context structure; FerrisSearch source and tests still
govern FerrisSearch behavior.
