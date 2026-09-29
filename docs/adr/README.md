# Architecture Decision Records

This directory holds FerrisSearch architecture decision records (ADRs). An ADR
records one significant decision: the context, the options considered, the
decision, and its consequences. The authority order is set in
[`../architecture-roadmap.md`](../architecture-roadmap.md#1-authority-and-how-to-read-this-document).
An accepted ADR refines or supersedes roadmap text; source code and tests still
describe current behavior.

## Status values

- **Proposed:** drafted for review. It describes intended behavior, not
  implemented behavior.
- **Accepted:** approved. Implementation tasks reference it by number.
- **Rejected:** declined. The record stays as evidence for the decision.
- **Superseded:** replaced by a later ADR, which it names.

A record that contains several decisions can accept one decision before the
others. That decision carries its own status line, and the record's status
covers the rest. Implementation status is tracked separately from acceptance.

## Required sections

The roadmap requires every strategic decision record to contain:

1. Context.
2. Alternatives considered.
3. The chosen decision.
4. Consequences.
5. Migration impact.
6. Affected roadmap gates.
7. Evidence that would invalidate the decision.

Records in this directory also cite current behavior by source file and name
the backlog tasks that implement each decision.

## Index

| ADR | Title | Status | Backlog |
|---|---|---|---|
| [0001](0001-write-consistency-and-retry-contract.md) | Write consistency and retry contract | Proposed; D1 accepted | FS-001 |
