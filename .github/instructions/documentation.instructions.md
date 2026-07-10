---
description: "Use for README, architecture and roadmap documents, benchmark reports, agent instructions, and public compatibility or maturity claims."
applyTo: "README.md,docs/**,.github/**/*.md,AGENTS.md,CLAUDE.md"
---

# Documentation And Evidence Instructions

## Authority And Audience

- Source and tests define implemented behavior.
- `docs/architecture-roadmap.md` defines intended direction and release gates.
- README is a concise public entry point, not the complete manual.
- `docs/next-50-tasks.md` is a ranked execution backlog, not a list of shipped
  features.
- `.github/instructions/` contains scoped engineering rules.
- Historical design notes must carry a visible status banner when they are not
  authoritative.

Do not duplicate long subsystem specifications across these layers. Link to the
canonical owner and keep one source of each fact.

## Current, Experimental, And Future

Use explicit tense and status:

- "supports" only for a path exercised by current source;
- "experimental" for implemented paths missing production protocol or evidence;
- "planned" or "roadmap" for unimplemented work.

Never convert a roadmap target into present-tense marketing. FerrisSearch is
pre-1.0 and not yet a production-safe drop-in OpenSearch replacement.

Use "OpenSearch-style REST API subset" unless a tested compatibility matrix
supports a more specific claim. Name unsupported semantics that materially
affect migration: concurrency control, full bulk semantics, PIT/snapshots,
aliases, templates, cluster APIs, and lifecycle management.

## Benchmark Integrity

Every performance claim needs:

- exact commit or version;
- release/debug profile and feature flags;
- hardware and operating system;
- node count and topology;
- dataset source, size, schema, and ingestion method;
- query text/request, concurrency, warmup, and run count;
- whether caches are cold or warm;
- result-correctness checks; and
- a reproducible command or script.

Do not compare unlike durability, replication, cache, correctness, or query
semantics without making the difference prominent. Label exploratory results
as exploratory. Prefer ranges/distributions over a single best run.

Do not hard-code total test counts in long-lived documentation. They drift as
normal tests are added. Name suites and record observed results in releases or
benchmark artifacts.

## README Standard

The README should answer, in order:

1. What FerrisSearch is and why it exists.
2. What can be demonstrated now.
3. How the two current engine modes differ.
4. How to run a small reproducible example.
5. What is experimental or missing.
6. Where to find architecture, roadmap, API, benchmarks, and contribution info.

Keep detailed endpoint catalogs, configuration references, and architecture
contracts in `docs/`. Use repository-relative links only; never local absolute
filesystem paths.

## Roadmap And Backlog Maintenance

- Change roadmap gates only when architecture, dependencies, or release
  criteria change.
- Change backlog rank when evidence changes urgency or dependency order.
- Do not mark a task done from code presence alone; verify its stated done
  criteria.
- Preserve task IDs so future sessions and issues can refer to stable entries.
- New feature ideas go after correctness and lifecycle blockers unless they
  unblock a roadmap gate.

## Agent Instruction Maintenance

Repository-wide agent context must stay short and broadly applicable. Put
file-specific traps behind `applyTo` frontmatter. Do not copy every struct,
endpoint, or test count into the global file.

When implementation changes:

1. update the scoped instruction that owns the invariant;
2. update the global file only if the rule applies to most work;
3. update `AGENTS.md`/`CLAUDE.md` only if discovery changes; and
4. run the instruction and link checks described in `docs/ai-agent-guide.md`.

## Review Checklist

- Verify symbols, paths, defaults, and limitations against source.
- Check all relative links and heading anchors.
- Search changed docs for stale version/test counts and broad compatibility
  claims.
- Ensure examples are syntactically complete and use current endpoints.
- Ensure secrets, personal paths, private hostnames, and benchmark credentials
  are absent.
- Inspect the final diff for accidental scope expansion or contradictory status
  labels.
