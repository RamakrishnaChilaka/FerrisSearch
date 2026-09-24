# Recovery Protocol Acceptance Matrix

> **Status: Proposed protocol acceptance; limited RP-1 coverage is recorded below.**
>
> **Date:** September 24, 2026.
>
> Contract: [Shard replication and recovery](recovery-protocol.md).
> Source baseline: `e805f70ff5dba0be9077b9bc32fcd488e837e6d1`.

Every row is an acceptance requirement. Existing tests may supply setup or
partial coverage, but no row is considered passed until evidence from the final
protocol implementation is recorded. The small crash probe that motivated this
work is not a substitute for this matrix.

## Harness And Evidence Rules

- **U:** deterministic state-machine/storage unit test.
- **T:** actual transport boundary, including serialization, deadlines, and errors.
- **P:** isolated real processes, distinct durable directories, real Raft/gRPC.
- **F:** fault-capable filesystem/storage harness for partial writes, fsync errors,
  corruption, and crash-persistence ordering. SIGKILL alone is not a power-loss test.
- **B:** bounded-resource measurement with recorded limits and workload.

Use named fault points and observable barriers, not sleeps as proof of ordering.
Timeouts bound a hung test; they do not prove a race occurred. The controller
records which boundary was reached before releasing, delaying, or killing work.
Network controls must distinguish metadata links, replication links, and client
ingress so asymmetric partitions can be reproduced.

The oracle records submitted operations, definite pre-mutation rejections,
successful receipts, and indeterminate outcomes. It checks exact document values,
deletes, versions, operation identities, and sequence history, not only counts.
An indeterminate operation may be present or absent; an acknowledged operation
must survive in canonical history and take effect according to later operations.
A later acknowledged delete correctly supersedes an earlier acknowledged insert.

For each execution retain source commit, Rust/dependency versions, features,
topology, durable-copy policy, durability mode, fault seed and schedule,
receipts, routing/term/state transitions, durable prefixes, snapshot manifests,
resource measurements where relevant, and terminal errors. Use isolated ports
and directories and stop only task-owned processes.

Every safety case below targets request durability unless it explicitly tests
the weaker asynchronous mode. Production parity requires all applicable rows,
not a percentage of green tests.

## Unit And Integration Test Ownership

Each implementation package includes the tests for its changed contract; tests
are not postponed until RP-6. RP-6 reruns and combines previously established
coverage on the complete protocol.

| Test surface | Unit tests (UTs) | Integration tests (ITs) |
|---|---|---|
| Authority and routing | Conditional Raft commands, term/generation transitions, allocation identity, replica-loss accounting, serialization and old-format rejection. | Real Raft application, follower forwarding, concurrent membership/settings changes, and cross-node propagation. |
| WAL and checkpoint progress | Sequence zero, overflow, gaps, no-op resolution, checksums, duplicate/conflicting records, prefix reconstruction, and pruning/pin interleavings. | Engine/WAL restart and replay; operation receipts and exact state across primary and replica processes. |
| Write and replica apply | Version/tombstone ordering, OCC conflicts, bounded retry records, and authority-permit state transitions. | Real gRPC duplicates/delays, replication errors, stale terms, coordinator retries, and per-item bulk outcomes. |
| Snapshot installation | Manifest validation, safe-boundary selection, chunk/offset/path checks, generation publication, and cleanup decisions under injected I/O failures. | Snapshot transfer, interrupted install/restart, vector/document agreement, and suffix replay on independent source/target directories. |
| Recovery state machine | Legal/illegal transitions, session replacement, cancellation, retention expiry, and budget accounting. | Catch-up with concurrent writes, admission barriers, stale completion, source failover, rejoin, and complete redundancy restoration. |
| Public behavior | Request validation and precise error/receipt encoding. | HTTP requests through a non-primary coordinator; verify values, deletes, metadata, and indeterminate outcomes after failures. |

Use existing module-local `#[cfg(test)]` conventions for UTs and existing
consensus/replication/REST/restart integration harnesses where they exercise the
required boundary. Add a focused process-backed recovery suite only when the
existing harness cannot express a case without mixing unrelated workflows.
RP-1 has the bounded executable evidence recorded below. The remaining proposed
protocol packages do not yet have executable acceptance implementations.

For a reproduced defect, first demonstrate that its focused regression fails
on the old behavior, then retain it with the correction. New protocol cases
must assert the injected boundary was reached and inspect final state, not
only task status, a planner flag, a log line, or a mocked call count. Distributed
safety requires actual transport/process evidence in addition to UTs.

Keep CI deterministic and bounded. Do not commit permanently failing placeholder
tests, hide required cases behind `#[ignore]`, or equate an environment-gated
early return with an exercised integration test. Run focused UTs/ITs during
each package, then the applicable feature and full gates on its final code.

## A. Allocation And Basic Failover

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| A01 | One node is primary for one shard and replica for others in the same index; kill it. | Each affected shard independently records its lost copy and correct remaining assignments. | U, P |
| A02 | Add/rejoin a replacement after A01 while ingestion continues. | Desired redundancy is restored; replacement is not in sync or promotable before verified catch-up. All acknowledged history remains represented. | P |
| A03 | Lose multiple nodes in one detection pass. | No duplicate promotion, stale-state overwrite, duplicate allocation, or incorrect replica-slot count. | U, P |

### RP-1 Evidence Record (September 24, 2026)

This records only the bounded current-behavior accounting package. It does not
certify the proposed recovery protocol, safe rejoin, admission, fencing,
acknowledgement policy, or production parity.

| ID | Implemented evidence | Current limit |
|---|---|---|
| A01 | `cluster::state::tests::remove_node_accounts_mixed_primary_and_replica_roles_per_shard`; process-backed `mixed_role_node_loss_accounts_every_shard_and_preserves_acknowledged_data` in `tests/restart_regression.rs`. | Covers one real elected-master loss with 3 shards, 2 replicas, no replacement node, preserved acknowledged values/deletes, and a post-promotion write under existing semantics. |
| A03 | `cluster::state::tests::sequential_node_removals_use_updated_routing_without_double_counting`, plus replica-only and no-replica helper boundaries. | Unit coverage only. A live same-pass multi-node/quorum-loss process case remains unimplemented and is not claimed. |

## I. Identity And Contiguous Progress

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| I01 | Empty copy and a copy containing only sequence zero. | Their prefixes are 0 and 1 respectively; full recovery includes sequence zero. | U, T, P |
| I02 | Deliver sequences 0, 2, then 1, including an index/delete pair for the same document. | Prefix stops at 1 until sequence 1 is resolved; no success for position 2 before prefix 3; exact values/deletes match the oracle, not arrival order. | U, T |
| I03 | Duplicate a valid operation/chunk and then reuse its identity with different content. | Duplicate returns its original outcome without a new sequence; conflicting reuse fails before mutation. | U, T |
| I04 | Recreate an index with the same name; deliver old requests. | Index UUID/history/allocation mismatch rejects them without affecting new data. | T, P |
| I05 | Replace a node's data directory while retaining its node name. | New storage has a new allocation and cannot inherit old in-sync/promotion eligibility. | P |
| I06 | Delay session messages across target restart or session replacement. | Old incarnation/recovery ID cannot apply or finalize state. | T, P |
| I07 | Reach sequence/term/generation exhaustion or receive malformed identities. | Checked rejection before WAL/metadata mutation; no wraparound or fabricated defaults. | U, T |
| I08 | Restart a nonempty copy with gaps and a persisted checkpoint. | Reconstructed contiguous progress agrees with verified storage, not maximum observation or an initialized zero. | U, F, P |

## W. Write Outcomes And Retry

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| W01 | Concurrent single/bulk/update/delete calls through different coordinators. | Each operation keeps its own term/sequence/ID and result on primary and replicas. | T, P |
| W02 | Primary durable mutation followed by replica timeout/error. | Affected request is failed/indeterminate with the actual cause, never a false success or guaranteed rollback. | T, P |
| W03 | Drop the primary's response to the coordinator after required copies persist the operation; retry the same internal envelope within its retained epoch. | Original mutation identity/result is reused without another sequence, but success requires current authority and required-copy proof. A fresh external request without that identity is not assumed deduplicated. | T, P |
| W04 | Retry after the supported retention window. | Explicit expired/unknown outcome; the internal retry is not silently accepted as a new write. | U, T |
| W05 | Conditional update/delete races with the same expected term/sequence. | Exactly one conflicting mutation wins; restart/promotion does not reset the version contract. | T, P |
| W06 | Validate malformed bulk items and interrupt a partially completed bulk. | Per-item receipts/errors remain attributable; successful items survive, failed/indeterminate items are not relabelled successful. | T, P |
| W07 | Skew clocks, flush/truncate the WAL within a retained epoch, promote a primary, install a snapshot, and retire the epoch. | Supported result/digest records survive flush, truncation, promotion, and install until committed retirement; an expired envelope is rejected, not reapplied, regardless of local clock readings. | U, T, P |
| W08 | Retry while a required replica lacks the operation, after exclusion commits, on an obsolete primary, and on the new primary after adoption. | A local dedup hit cannot create success. Only current ACTIVE authority plus required-copy durable-prefix proof can confirm the original mutation without reallocation. | T, P |

## D. Durable Storage Boundaries

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| D01 | Crash at WAL reservation, frame write, fsync, engine apply, replica response, and client response. | Success implies recoverable data on every required copy; unacknowledged outcomes are classified accurately. | F, P |
| D02 | Error after a real WAL record is durable but before engine apply finishes. | Copy becomes unavailable/reconstructs; the record is not overwritten with a no-op to hide failure. | U, F, P |
| D03 | Reserve a position without completing a mutation, then advance later writes. | A durable resolution/no-op closes the gap before prefixes advance; no conflicting operation reuse. | U, T |
| D04 | Lose every required process immediately after success, then restart the intact disks. | Exact acknowledged state and operation metadata survive using the declared fsync/format contract. | F, P |
| D05 | Repeat a crash with asynchronous fsync enabled. | Evidence and status identify the weaker mode; no claim that acknowledged-loss guarantees were exercised. | F, P |
| D06 | Inject EIO/ENOSPC during primary or replica WAL write, fsync, or frontier publication; separately create a torn tail and mid-log corruption. | Storage error fail-stops the copy with no subsequent ack or position reuse. Only a proven unsealed tail beyond the durable frontier may be discarded; corruption/ambiguous frontiers prevent eligibility. | U, F, P |

## F. Fencing And Promotion

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| F01 | Kill a primary with eligible replicas and metadata quorum. | Only an eligible verified copy activates; acknowledged operations survive and new sequence allocation is continuous. | P |
| F02 | Partition an old primary from the metadata majority; let a valid candidate promote. | Old primary cannot obtain a new obsolete-term success proof; delayed old replication cannot overwrite new history. | T, P |
| F03 | Pause a replica apply just before mutation, install a higher-term fence, then release it. | Permit/fence ordering either includes the old operation before the barrier or rejects it without mutation. | U, T |
| F04 | Candidate has no local observations of other copies, or a stale copy reports the largest sequence. | Neither condition grants eligibility; first-replica and maximum-checkpoint fallbacks are absent. | U, T, P |
| F05 | Crash the candidate after `PROMOTING`, after its durable fence, and before/after `ACTIVE`. | Higher-term retry preserves canonical history; no two primaries obtain conflicting success proofs. | F, P |
| F06 | A late response for an operation already copied to the candidate arrives after promotion. | The operation is retained; delayed response delivery is not mistaken for proof that a stale new mutation was accepted. | T, P |
| F07 | No current in-sync copy survives, but a stale copy is reachable. | Automatic recovery remains unavailable; no acknowledged-loss promotion. Explicit data-loss recovery, if later supported, creates a new history. | P |
| F08 | Isolate metadata quorum, including losing one voter from a two-voter metadata configuration. | Reconfiguration follows actual Raft quorum rules; data-copy count is never treated as metadata authority. No minority promotion. | P |
| F09 | Delay sequence s, deliver s+1, lag global-prefix propagation, then crash the primary at each ack boundary. | No success above an unresolved gap; promotion preserves the candidate's own durable acknowledged history even when the known global prefix is lower. | T, P |
| F10 | Give an old eligible replica a conflicting tail and an older snapshot; crash/supersede promotion during reconciliation, including conflicting peer records where the candidate lacks a position. | No authoritative copy is destructively rolled back before committed exclusion/activation. Missing-candidate conflicts use the declared no-op/exclusion rule; every later eligible promotion preserves acknowledged history. | F, P |
| F11 | Crash a primary with non-conflicting in-flight records spread across survivors; restart a reporting peer and deliver old-term work; after promotion, lose the new primary's shard storage while retaining metadata quorum. Also exercise genuine conflicts separately. | Durable peer fences reject delayed old-term work; new incarnation requires a fresh report. Normal tails preserve redundancy by append-only resync. Genuine-conflict fallback enforces the selected minimum and reports unavailable after loss of the only eligible copy. | T, F, P |

## M. Membership And Acknowledgement Sets

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| M01 | A required copy fails; the primary attempts exclusion. | Exclusion requires conditional Raft authority; failure/timeout cannot edit a local-only in-sync set. | U, T, P |
| M02 | Exclusion races with promotion or another configuration update. | Expected-term/generation checks reject stale changes; acknowledgement-set intersection remains valid. | U, T, P |
| M03 | Lose the response to an exclusion/admission Raft command. | Admission remains blocked until authoritative reconciliation; no guessing which set committed. | T, P |
| M04 | Admit a recovered target while concurrent writers are queued. | All pre-barrier operations are durable on target; every post-barrier success includes it in the required set. | T, P |
| M05 | Remove replicas until only one remains; test minimum copies 1 and 2. | Mode 1 accepts only under valid committed authority; mode 2 blocks writes until a second durable eligible copy exists. | U, P |
| M06 | Simultaneous mapping/settings and shard membership changes. | Conditional per-shard metadata mutation preserves unrelated committed fields and reaches followers through Raft. | T, P |
| M07 | Delay AddInSync commit beyond its timeout and a linearizable read of the old generation; then release it, including while the primary is partitioned from Raft. | The old-generation read never resumes writes. A committed transition or competing generation-bumping CAS settles the result; required ack membership never omits a committed in-sync copy. | T, P |
| M08 | Delete an index while its sole primary/stale coordinator are partitioned; race delayed promotion/admission/exclusion, attempt recreation and stale writes, and leave a holder permanently lost. | DELETING fences old configuration commands and reserves the name. Deletion/recreation stay pending without proof, indefinitely if necessary; no timeout override, revived permits, or silent retargeting. | T, P |

## S. Snapshot Integrity And Atomic Installation

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| S01 | Snapshot while writes/update/delete operations continue. | Manifest prefix describes exactly the included state; no later mutation is hidden behind an earlier prefix. | U, P |
| S02 | An engine commit contains operations above the global safe boundary. | It is not advertised as a snapshot of that earlier boundary; preparation selects or establishes a valid safe commit. | U, T |
| S03 | Crash around each file fsync, directory fsync, and active-pointer rename. | Restart selects complete old or complete new state; no mixed engine/version/vector generation. | F, P |
| S04 | Corrupt/truncate a manifest, file, checksum, or chunk. | Session fails with a specific cause before install/admission; previous valid state remains usable. | U, T, F |
| S05 | Duplicate/reorder chunks; submit overlapping offsets or path traversal. | Valid duplicates are idempotent; malformed/conflicting/path-escaping input fails before altering active state. | U, T |
| S06 | Recover deletes and multiple versions of one document. | Final values, absence, tombstones, and OCC metadata match the snapshot plus canonical suffix. | P |
| S07 | Recover a vector shard larger than the former rebuild cap. | Every live vector matches its document/version; deleted vectors stay deleted. Rebuild failure keeps copy unavailable. | T, P |
| S08 | Schema/format mismatch and unsupported old-format data. | Explicit conversion/reindex requirement or compatible verified path; never default history/term/schema to accept it. | U, T, P |
| S09 | Merge cleanup or delete-index races with an active snapshot transfer. | Pins prevent use-after-delete; deletion cancels the specific session and cleans up only after references are released. | T, P |
| S10 | Prune tombstones/version history while old retries, pinned snapshots, and delayed recovery records still exist. | A durable rejection floor and safe snapshot prevent old-value resurrection; unsupported old replay requires snapshot recovery, and pinned old views remain complete. | U, T, P |

## H. History Retention

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| H01 | Replica returns within a valid retained-history window. | Operation-only recovery uses the verified common prefix and includes all necessary operations. | T, P |
| H02 | Replica returns after required history has been pruned. | Explicit snapshot fallback; an empty WAL response is not interpreted as caught up. | T, P |
| H03 | WAL pruning races with snapshot/session pin acquisition. | The session either owns the required history or fails before promising replay; no lease over deleted data. | U, F |
| H04 | Pinned history reaches its byte/time budget. | Explicit cancellation/backpressure according to policy, bounded storage, and later safe snapshot restart. | T, P, B |
| H05 | Source fails and the replacement lacks its retention lease/history. | New session revalidates actual history; no assumed transfer of the old source's guarantee. | P |

## R. Streaming, Divergence, And Rejoin

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| R01 | Start from no snapshot and no operations. | Snapshot bootstrap plus suffix reaches exact oracle state, including operation zero. | T, P |
| R02 | A nonzero-checkpoint replica misses subsequent operations. | Automatic catch-up repairs it; recovery is not restricted to checkpoint zero. | T, P |
| R03 | The metadata leader itself hosts a lagging replica. | Same recovery state machine runs regardless of metadata-leader role. | P |
| R04 | Target contains a conflicting old-primary update/delete tail. | Divergence is detected; rollback/replacement removes that tail before canonical replay. A larger checkpoint does not win. | T, P |
| R05 | Fail index/delete apply or deliver malformed payload mid-stream. | Session terminates at that operation, preserves the last valid prefix, and cannot enter in-sync state. | U, T |
| R06 | Primary/source changes during file copy, replay, or finalization. | Old session fails; safe reuse requires a new authority handshake; delayed completion is rejected. | T, P |
| R07 | Lose connections repeatedly and retransmit acknowledged chunks. | Restart makes bounded progress without duplicate effects, skipped ranges, or unbounded retry state. | T, P, B |
| R08 | Writes cross snapshot, streaming, and final admission boundaries. | No missing or duplicate logical mutation; old base is never mixed with live updates. | T, P |
| R09 | Source cannot provide a valid snapshot or complete history. | Shard/copy remains explicitly unavailable; no partial-result recovery success. | T, P |

## O. Operational Bounds

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| O01 | Recover data much larger than configured in-flight memory with many chunks. | Accounted memory remains within configured reservations plus measured fixed overhead; no data-sized `Vec` fallback. | B, P |
| O02 | Start more recoveries than node/session permits. | Bounded admission/rejection; aggregate bytes obey node limits, not just per-session limits. | T, B |
| O03 | Exhaust staging disk or WAL retention quota mid-transfer. | Diagnosable failure and cleanup; intact active copy is not deleted to manufacture space. | F, P, B |
| O04 | Cancel or expire a recovery deadline at every phase. | Child RPC/file/replay work stops; buffers, pins, permits, and staging cleanup reach a bounded terminal state. | T, P, B |
| O05 | Run recovery alongside foreground requests and Raft heartbeats. | Recovery cannot occupy all steady-state workers or block control-plane progress; record latency/resource behavior rather than assert an unmeasured speedup. | P, B |
| O06 | Observe success, retry, failure, and cancellation. | Status/metrics distinguish phase and cause; high-cardinality IDs stay out of metric labels; no success event before admission commits. | T, P |

## E. End-To-End Acceptance And Claim Boundaries

| ID | Boundary or scenario | Required result | Layer |
|---|---|---|---|
| E01 | Three-node writes; crash primary; continue writes; restart old primary; restore redundancy. | Exact final oracle state on all admitted copies, continuous history, stale tail rejected/repaired. | P |
| E02 | Partition/heal repeatedly with concurrent updates/deletes and response loss. | Every success has a valid canonical placement; no acknowledged-history loss or stale overwrite across terms. | P |
| E03 | Repeat deterministic schedules across multiple seeds and restart boundaries. | Reproducible results and minimized failing schedules; no timing-only flakes accepted as evidence. | U, T, P |
| E04 | Compare incremental versus full-copy recovery on equivalent datasets and limits. | Record data size, history gap, hardware, versions, durability, topology, traffic, bytes, memory, throughput, and distributions. No parity claim from one best run. | P, B |
| E05 | Run supported plaintext and encrypted transports through failure/rejoin cases. | Same safety outcomes; transport errors retain diagnosis. External-service cases explicitly identify whether actually exercised. | T, P |
| E06 | Audit publication claims and final artifacts. | Every claimed guarantee names its fault model, copy policy, completed cases, exact source revision, and remaining limits. No full industry-parity claim from a passing subset. | Evidence review |

## Completion Record

Implementation PRs should link durable results for the cases they cover, identify
partial coverage, and retain failing schedules. Do not change `Proposed` to
`Verified` because a test function exists or a reviewer approved the design.

The current design does not set production recovery-time or foreground-latency
targets without measurements. Resource limits themselves are testable contracts:
report configured budgets, observed usage, fixed overhead, and violations.
Performance certification is a later result, separate from safety certification.
