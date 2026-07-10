---
description: "Use for the ferris-cli SQL console, streamed NDJSON handling, query parsing, completion, watch mode, table rendering, and CLI metadata."
applyTo: "src/cli.rs"
---

# Ferris CLI Instructions

## Current Role

`src/cli.rs` implements the `ferris-cli` SQL console. It supports interactive
and one-shot SQL, multiline input, persistent history, completion, `\watch`,
table rendering, EXPLAIN presentation, and consumption of global
`POST /_sql/stream` NDJSON responses.

The CLI is an API client, not a second SQL planner. Keep SQL semantics,
identifier resolution, and execution decisions on the server.

## Stream Protocol

- A successful stream starts with exactly one `meta` frame, followed by zero or
  more `rows` frames.
- Rows before metadata, duplicate metadata, unknown frame types, malformed JSON,
  and trailing incomplete frames are errors.
- Preserve chunk-boundary correctness; a network chunk is not an NDJSON frame.
- Keep `matched_hits`, returned row count, `truncated`, `streaming_used`,
  execution mode, approximate top-K state, and grouped timing meanings
  distinct.
- An error frame must be shown as failure, not an empty successful result.

## Parsing And Rendering

- Prefer `sqlparser` for statement structure. String fallbacks must be narrow,
  case-insensitive where SQL requires it, and covered with multiline and quoted
  identifier tests.
- Do not auto-quote or rewrite output aliases as source tables/fields.
- Quoted table names, including hyphens, must survive global SQL routing.
- Rendering must not panic on nulls, missing optional metadata, large numbers,
  empty rows, or unknown future response fields.
- User-facing timing labels must identify server stages vs client wall time.
- Treat approximate top-K as approximate whenever API metadata says it was
  used; do not infer exactness from query shape alone.

## Interactive Behavior

- Keep pure helpers for completion boundaries, shell-command parsing, watch
  intervals, history eligibility, and metadata labels.
- `\watch` reuses the parsed SQL and has a positive validated interval.
- One-shot mode returns a non-zero exit on transport, protocol, or server error.
- Do not print credentials, auth headers, or full response bodies that may
  contain secrets.
- Authentication flags are not currently part of the CLI contract; if added,
  wire them through every request and add redaction tests.

## Tests

Add focused unit tests for parser/rendering helpers rather than relying on a
terminal. Stream changes require frame-order, malformed-frame, and arbitrary
chunk-boundary coverage. API metadata changes require tests for missing and
present forms so older/newer server versions degrade safely.
