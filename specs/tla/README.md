# FerrisSearch shard replication and peer-recovery model

This directory contains a bounded TLA+ model of one FerrisSearch
`local_shards` shard at source baseline `8f17172` (merged PR #143).

**Scope of the guarantee:** TLC exhaustively checks every behavior reachable
within each configuration's stated finite bounds. A passing configuration is
not a proof for arbitrary cluster sizes, write counts, failures, or time.

## Run the model

Java 25 is used in CI. The runner downloads TLA+ tools 1.7.4 and verifies:

```text
sha256 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
```

Run the complete fast matrix:

```bash
./scripts/tla/check.sh
```

Run selected configurations:

```bash
./scripts/tla/check.sh c1-aba c1-aba-fixed c2 l1
```

Use an existing verified jar or retain raw logs:

```bash
TLA2TOOLS_JAR=/path/to/tla2tools.jar \
TLA_LOG_DIR=/path/to/logs \
./scripts/tla/check.sh c1-aba-fixed
```

`./scripts/tla/check.sh --list` prints all names and expected outcomes. The
runner gives every invocation isolated TLC and Java temporary directories. It
fails when an expected-pass configuration reports an error, or when an
expected counterexample no longer violates its named invariant.

Deadlock checking is disabled because the finite write/fault/recovery bounds
create intentional terminal states. Safety configurations use a state
constraint and, where no node role is distinguished, node symmetry. The
liveness configuration uses neither symmetry nor a state constraint.

## Modules

| Module | Responsibility |
| --- | --- |
| `RaftLog.tla` | Ordered conditional command log, explicit voters and leader, quorum-aware commits, leader-applied writes, lagging follower views. |
| `ShardReplication.tla` | Primary activation, validated writes, sequence allocation, synchronous in-sync replication, promotion semantics, and optional allocation identities. |
| `PeerRecovery.tla` | Asynchronous source setup, snapshot boundary and pin, atomic verified install, suffix catch-up, exclusive finalize barrier, settlement, persistent pending observation, abort, and expiry. |
| `Faults.tla` | Crash/restart, elections, metadata partitions, message loss, ordered dead-node lifecycle, rejoin/allocation, disk loss, flush/truncation, and asynchronous durability loss. |
| `Invariants.tla` | Safety and liveness properties. |
| `MC_C2.tla` | Canonical three-voter stale-primary scenario with equivalent role permutations removed. |
| `MC_C3.tla` | Canonical same-node disk-loss scenario. |
| `MC_C4.tla` | Canonical asynchronous-durability crash scenario. |
| `MC_L1.tla` | Fault-free recovery progress actions and weak-fairness assumptions. |

## Core abstractions

- One shard is modeled. Every node has at most one local copy.
- Documents are represented by bounded document keys and unique write IDs.
  Deletes are distinct write kinds; value/rollback checks use operation
  identity and exact sequence numbers.
- At most one client write is active at once. That write still interleaves with
  Raft, replication, recovery, crashes, view delivery, and message loss.
- The initial primary term is normalized to 1 after its initial activation.
  Promotion and later activation preserve the implemented relative term
  ordering.
- Raft requires a live connected leader and a live majority of current voters.
  The leader applies a committed command before its synchronous write returns.
  Other nodes nondeterministically apply a committed-log prefix.
- Proposal invocation order is not log order. A request may be delayed before
  reaching the leader and be appended after later requests.
- A data-plane RPC may remain delayed or be lost. It is not duplicated unless
  the modeled caller retries.
- Request durability places each successful WAL append in `durableOps`.
  C4 leaves appends volatile until `Flush`.
- Snapshot files, hard links, chunk hashes, directory fsyncs, and strict
  Tantivy open are abstracted as one atomic verified file-set installation.
  Snapshot boundary capture, committed checkpoint, and WAL retention pin are
  modeled separately.
- Recovery operation batches are represented one operation at a time.
  Sequence numbers strictly increase, but gaps are allowed as in
  `apply_recovery_operations`.
- The source permits one active recovery session for the shard. The target
  enters destructive `Recovering` state only after asynchronous setup
  completes.
- Wall-clock durations are abstracted as nondeterministic timeout actions.
  Trace documentation explains when a counterexample also fits implemented
  timing bounds.

## Action-to-code mapping

| Model action | Merged Rust implementation |
| --- | --- |
| `ClientWrite` | Coordinator routing in `src/api/index/`. |
| `PrimaryAccept`, `PrimaryReject`, `PrimaryAck`, `PrimaryFail` | `TransportService::{index_doc,bulk_index,delete_doc}`, including `ensure_primary_activated`, `peer_recovery_write_guard`, `validated_primary_write_state`, and all-in-sync acknowledgement. |
| `ReplicaApply`, `DeliverReplicaAck` | `TransportService::{replicate_doc,replicate_bulk}` and `replication::{replicate_write,replicate_bulk}`. |
| `ProposeActivate`, `ObserveActivation`, `CancelActivation` | `TransportService::ensure_primary_activated`. |
| `CommitRaft` | `ClusterStateMachine::apply_command`; rejected conditional commands retain a log position without changing routing. |
| `DeliverView` | Per-node `ClusterManager` observation of an applied Raft prefix. |
| `ElectLeader` | openraft leader election, abstracted to a live connected voter with quorum. |
| `SuspectAndRemove`, `ObserveRoutingAccepted`, `ObserveRoutingRejected` | Leader dead-node loop in `src/node/mod.rs`, `IndexMetadata::{remove_node,select_promotion_candidate,promote_replica_to}`, and checked `UpdateIndex`. |
| `ChangeRaftMembership`, `ProposeRemoveNode`, `ObserveNodeRemoved` | `Raft::change_membership`, followed by `ClusterCommand::RemoveNode`; removal is deferred after rejected routing updates. |
| `Rejoin`, `ObserveRejoin` | Follower `JoinCluster` retry and committed `ClusterCommand::AddNode`. |
| `AllocateAfterLifecycle`, `ObserveAllocationAccepted`, `ObserveAllocationRejected` | `IndexMetadata::allocate_unassigned_replicas` and the allocator phase of the leader lifecycle loop. |
| `StartRecovery`, `SourceSetupFailure`, `PollSetupFailure` | `run_peer_recovery`, `start_peer_recovery_inner`, `launch_source_setup`, and `source_start_status`. |
| `SourceSnapshot` | `HotEngine::prepare_peer_recovery_snapshot`, including commit, durable checkpoint, hard-linked files, and `register_retention_pin`. |
| `TargetBeginInstall`, `InstallSnapshot` | `ShardManager::{begin_peer_recovery_target,prepare_peer_recovery_target_blocking,finalize_peer_recovery_target_blocking}`. |
| `FetchOps`, `ApplyOps`, `FinishCatchUp` | `fetch_recovery_ops_inner` and `apply_recovery_operations`. |
| `BeginPrepareFinalize`, `CancelPrepareFinalize`, `AcquireFinalizeBarrier`, `FinishFinalizeTail` | `prepare_finalize_recovery_inner` and `FinalizePreparingGuard`. |
| `TargetComplete` | `mark_peer_recovery_awaiting_membership_blocking`. |
| `BeginSettlement`, `ProposeMarkInSync`, `SettlementDeadline`, `ObserveAdmission` | `complete_finalize_recovery_inner`, `settle_peer_recovery`, `submit_mark_replica_in_sync`, `submit_settlement_term_bump`, and `observe_membership`. |
| `TargetObserveAdmitted`, `TargetObserveRejected` | `observe_target_membership` and `PeerRecoveryDriver::reconcile_pending_targets`. |
| `AbortSession`, `ExpireSession`, `ExpireFinalizeWithoutMark` | `abort_shard_session`, `reap_expired_sessions`, `reap_peer_recovery_sessions`, and `settle_abandoned_finalize`. |
| `Flush` | `HotEngine::flush_with_global_checkpoint` and `HotTranslog::{truncate,truncate_below}`. |
| `Crash`, `Restart` | Process loss/restart and WAL reopen/replay; volatile activation, barriers, and source sessions are discarded while durable pending markers survive. |
| `PartitionMetadata`, `LoseMsg` | Metadata-link isolation and delayed/lost transport requests. |
| `DiskLoss`, `OpenAssignedEmptyCopy` | Same-node restart with a missing shard directory; the allocation-ID variant rejects a missing durable local identity. |

## Allocation-ID variant

`AllocationIds = FALSE` models merged Rust behavior. Replica authority is keyed
by node name, index UUID, primary, and primary term.

`AllocationIds = TRUE` models a proposed fix:

1. every routed copy has a durable assignment ID;
2. removal clears that assignment;
3. reallocation uses a fresh monotonic ID derived from the Raft position;
4. the target sends the allocation ID from its own local view in
   `StartRecovery`;
5. the source rejects start until its current allocation ID exactly matches;
6. snapshot setup, the source session, the target pending marker, and
   `MarkReplicaInSync` retain that ID;
7. the state machine admits only an exact current allocation-ID match; and
8. target observation admits the same ID when in sync, admits promotion, and
   rejects a missing or different ID.

The Rust fix must implement this complete handshake. Adding only an allocation
field to `MarkReplicaInSync` is insufficient: the retained
[`stale-start trace`](traces/C1-allocation-id-stale-start-no-partial-serve.md)
shows why target and source must agree before snapshot setup.

## Properties

Safety invariants:

- `RoutingWellFormed`
- `NoAckedLoss`
- `PromotionComplete`
- `AdmissionComplete`
- `UniqueAckedSeq`
- `NoAckedRollback`
- `NoAuthoritativeWipe`
- `NoPartialServe`

Liveness properties under the explicit fault-free fairness assumptions in
`MC_L1.tla`:

- `BarrierReleased`
- `RecoveryConverges`
- `PendingResolves`

## Configurations and results

Results below were produced on September 26, 2026 with Java 25 and the pinned
TLA+ tools jar. Times are TLC wall times on one development host, not
performance benchmarks.

| Runner name | Nodes/docs/writes | Fault and protocol bounds | Allocation IDs | Expected/result | Generated / distinct | Depth | Time |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| `c1-fast` | 2 / 1 / 1 | 1 crash; no recovery; term 3; log 5; view lag 2 | Off | Pass | 3,340 / 999 | 15 | 1s |
| `c1-recovery` | 2 / 1 / 1 | 1 recovery; no crash; term 3; log 3; view lag 2 | Off | Pass | 25,149 / 6,734 | 29 | 2s |
| `c1-aba` | 3 / 1 / 0 | 1 recovery and crash; term 2; log 5; view lag 5 | Off | Expected `NoPartialServe` violation | 546,975 / 166,448 | 28 | 11s |
| `c1-aba-fixed` | 3 / 1 / 0 | 1 recovery and crash; term 2; log 6; view lag 5 | On | Pass | 2,619,823 / 713,276 | 45 | 43s |
| `c2` | 3 / 1 / 2 | 1 metadata partition; term 3; log 2; canonical primary/leader | Off | Expected `UniqueAckedSeq` violation | 46 / 33 | 19 | 1s |
| `c2-allocation-ids` | Same as C2 | Same as C2 | On | Expected `UniqueAckedSeq` violation | 37 / 33 | 19 | 1s |
| `c3` | 3 / 1 / 1 | 1 crash and disk loss; no metadata update | Off | Expected `NoAckedLoss` violation | 36 / 25 | 12 | 1s |
| `c3-allocation-ids` | Same as C3 | Missing local assignment identity fails closed | On | Pass | 28 / 24 | 11 | 1s |
| `c4` | 3 / 1 / 1 | 1 primary crash; async WAL durability | Off | Expected `NoAckedLoss` violation | 32 / 21 | 9 | 1s |
| `l1` | 2 / 1 / 0 | 1 recovery; no faults; weak fairness; no constraint/symmetry | On | Safety and all liveness properties pass | 20 / 17 | 15 | 1s |

The C2 configurations deliberately use canonical role constants rather than
symmetry reduction. Their next-state relation is restricted to the
partition-promotion-activation-write path needed to keep the living regression
well below the CI budget.

## Counterexamples

- [C1 same-node allocation ABA](traces/C1-allocation-aba-no-partial-serve.md)
- [C2 stale-primary duplicate sequence](traces/C2-stale-primary-unique-seq.md)
- [C3 same-node empty-disk reuse](traces/C3-disk-loss-no-acked-loss.md)
- [C4 asynchronous-durability loss](traces/C4-async-durability-no-acked-loss.md)
- [Why allocation-ID start must be a two-sided handshake](traces/C1-allocation-id-stale-start-no-partial-serve.md)

## Not covered

- This is not a proof for unbounded nodes, writes, terms, crashes, or queues.
- No Apalache inductive check has been run.
- No TLAPS proof has been written.
- Process-test protocol traces are not yet emitted or validated against TLA+.
- Index delete/recreate identity is abstracted as pre-finalize abort rather
  than modeled end to end.
- File/chunk/frame-size limits, SHA-256 implementation details, torn-frame
  decoding, legacy `RecoverReplica`, and vector rebuild are outside this model.
- Hard-link availability and filesystem crash atomicity are assumptions of the
  atomic verified-install abstraction.
- Timing is nondeterministic; the model checks ordering, not probability or
  recovery latency.
