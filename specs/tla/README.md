# FerrisSearch shard replication and peer-recovery model

This directory contains a bounded TLA+ model of one FerrisSearch
`local_shards` shard at source baseline `8f17172` (merged PR #143).

**Scope of the guarantee:** TLC exhaustively checks every behavior reachable
within each configuration's stated finite bounds. A passing configuration is
not a proof for arbitrary cluster sizes, write counts, failures, or time.

The retired diagnostic property `NoStaleReplicaApply` compared every apply
against the globally committed term. That was too strong for an operation
accepted and sent before promotion when the receiving copy had not yet
observed the promotion. The retained trace explains the correction:
[`Fixed-partition-prepromotion-inflight-apply.md`](traces/Fixed-partition-prepromotion-inflight-apply.md).

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
| `ShardReplication.tla` | Primary activation, validated writes, sequence allocation, synchronous in-sync replication, wire identity, optional allocation identities, and optional durable primary-term fencing. |
| `PeerRecovery.tla` | Asynchronous source setup, snapshot boundary and pin, atomic verified install, suffix catch-up, exclusive finalize barrier, settlement, persistent pending observation, abort, and expiry. |
| `Faults.tla` | Crash/restart, elections, metadata partitions, message loss, ordered dead-node lifecycle, rejoin/allocation, disk loss, flush/truncation, and asynchronous durability loss. |
| `Invariants.tla` | Safety and liveness properties. |
| `MC_C2.tla` | Canonical three-voter stale-primary scenario with equivalent role permutations removed. |
| `MC_C3.tla` | Canonical same-node disk-loss scenario. |
| `MC_C4.tla` | Canonical asynchronous-durability crash scenario. |
| `MC_L1.tla` | Fault-free recovery progress actions and weak-fairness assumptions. |
| `MC_L2.tla` | One target crash/restart followed by fault-free weakly fair recovery. |
| `MC_FenceDurability.tla` | Targeted proof that a learned replica fence must survive restart. |
| `MC_Fixed_Crash.cfg`, `MC_Fixed_Partition.cfg` | Unrestricted three-voter fixed-design safety checks with two writes, recovery, message loss/delay, and crash or partition faults. |
| `MC_Fixed_Simulation.cfg` | Larger seeded simulation profile for deeper randomized executions. |

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
- Replication messages carry sender primary term, index UUID, and target
  allocation ID. With `ReplicaFencing = FALSE`, the term is informational and
  models the merged implementation's missing term check.
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
| `ReplicaReject`, `DeliverReplicaNack` | Proposed pre-WAL identity/fence rejection in the same replica handlers and propagation as a synchronous replication failure. |
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

## Replica-fencing variant

`ReplicaFencing = FALSE` models the merged replica handlers, which accept
primary-originated operations without checking the sender's primary term.

`ReplicaFencing = TRUE` requires every `ReplicateDoc` and `ReplicateBulk`
operation to carry:

- the index UUID;
- the sender's activated primary term; and
- the target allocation ID when allocation identities are enabled.

Before WAL or engine mutation, the replica rejects the operation when:

- the index UUID differs from the local copy UUID;
- the target allocation ID differs from the current local assignment; or
- the message term is below `max(local cluster-view term, local replica fence)`.

An accepted operation raises the local fence to its term. A node whose view
shows that it became primary also raises the fence before activation and its
first write. `DurableReplicaFence = TRUE` persists those advances atomically
before acknowledging the triggering operation or activation. Restart restores
the durable fence before replica RPCs are accepted.

The retained
[`volatile-fence trace`](traces/Fence-volatile-restart-stale-probe.md)
shows why durability is required: a replica can learn term 3 from replication,
restart while its Raft view still says term 1, and otherwise accept a term-1
retry. The durable variant rejects the same probe.

### Required Rust implementation contract

The future Rust change must implement the model variant as one protocol:

1. Routing metadata assigns every primary and replica copy an allocation ID.
   Removing a copy clears its ID; every later assignment, including reuse of
   the same node name, receives a fresh monotonic ID.
2. `StartPeerRecoveryRequest` carries the allocation ID from the target's
   applied view. The source validates index UUID, primary node, primary term,
   target assignment, and exact allocation ID before snapshot setup.
3. Source session state, snapshot metadata, target install metadata, the
   durable awaiting-membership marker, `MarkReplicaInSync`, and its forwarded
   RPC retain the same allocation ID. Admission performs an exact comparison.
4. `ReplicateDocRequest` and every bulk replica operation carry index UUID,
   sender primary term, and target allocation ID. Missing fields fail closed;
   zero/default substitution is not permitted.
5. Before any WAL append or engine mutation, the replica resolves the local
   copy and validates, in order: index UUID, allocation ID, recovery gate, and
   `message_term >= max(applied_view_term, durable_replica_fence)`.
6. If an accepted message raises the fence, persist the new fence before
   acknowledging the operation. For bulk, validate the common identity/term
   before the first item mutates and persist the raised fence once for the
   accepted batch.
7. When an applied routing view makes a copy primary, durably raise its fence
   to the promoted term before activation completes and before the first
   primary write is permitted.
8. Restart loads and validates the persisted index UUID, allocation ID, and
   fence before accepting replica traffic. Missing or malformed identity/fence
   state on an existing authoritative copy fails closed.
9. Pending-target observation admits only the same allocation ID when it is
   in sync, or that same copy after promotion. A missing or different
   allocation ID is definitive rejection; otherwise the result remains
   unknown.
10. Any rejection is returned through the existing synchronous replication
    failure path; it must not be converted into a successful item or request.

The model updates fence and accepted operation atomically. Rust may persist the
fence immediately before the WAL mutation; a crash in between is conservative
because it can reject more old-term traffic but cannot acknowledge a mutation
without its fence.

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
- `NoApplyBelowObservedFence`
- `ActivePrimaryRejectsOldTerm`

The targeted fence-durability model also checks
`FenceRejectsStaleProbe`, and the C2 model checks
`C2RejectsStaleMessage`.

The retired `NoStaleReplicaApply` assertion and its counterexample remain in
the trace directory. It compared against unseen global state rather than the
replica's applied view and durable fence.

Liveness properties:

- `BarrierReleased`
- `RecoveryConverges`
- `PendingResolves`

`MC_L1.tla` assumes no faults and weak fairness for each recovery phase, Raft
commit, target view delivery, source admission observation, and target pending
observation. `MC_L2.tla` additionally makes one target crash, its restart, and
permanent cessation of faults weakly fair. The transient crash completes
before failure detection removes the assignment. Neither liveness
configuration uses symmetry reduction or a state constraint.

## Configurations and results

Results below were produced on September 26, 2026 with Java 25 and the pinned
TLA+ tools jar. Times are TLC wall times on one development host, not
performance benchmarks.

| Runner name | Nodes/docs/writes | Fault and protocol bounds | Allocation IDs | Expected/result | Generated / distinct | Depth | Time |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| `c1-fast` | 2 / 1 / 1 | 1 crash; no recovery; term 3; log 5; view lag 2 | Off | Pass | 3,340 / 999 | 15 | 2s |
| `c1-recovery` | 2 / 1 / 1 | 1 recovery; no crash; term 3; log 3; view lag 2 | Off | Pass | 25,149 / 6,734 | 29 | 3s |
| `c1-aba` | 3 / 1 / 0 | 1 recovery and crash; term 2; log 5; view lag 5 | Off | Expected `NoPartialServe` violation | 496,458 / 153,029 | 28 | 12s |
| `c1-aba-fixed` | 3 / 1 / 0 | 1 recovery and crash; term 2; log 6; view lag 5 | Allocation IDs + durable fencing | Pass | 2,619,829 / 713,284 | 45 | 47s |
| `c2` | 3 / 1 / 2 | 1 metadata partition; term 3; log 2; canonical primary/leader | Off | Expected `C2RejectsStaleMessage` violation | 18 / 16 | 14 | 1s |
| `c2-allocation-ids` | Same as C2 | Same as C2 | Allocation IDs only | Expected `C2RejectsStaleMessage` violation | 16 / 16 | 14 | 1s |
| `c2-fixed` | Same as C2 | Same as C2 | Allocation IDs + durable fencing | Pass | 28 / 20 | 16 | 1s |
| `fence-volatile` | 3 / 1 / 2 | 1 partition and replica crash/restart; term 3 | Allocation IDs + volatile fencing | Expected `FenceRejectsStaleProbe` violation | 17 / 17 | 15 | 1s |
| `fence-durable` | Same as volatile-fence check | Same schedule | Allocation IDs + durable fencing | Pass | 21 / 19 | 16 | 1s |
| `c3` | 3 / 1 / 1 | 1 crash and disk loss; no metadata update | Off | Expected `NoAckedLoss` violation | 36 / 25 | 12 | 1s |
| `c3-allocation-ids` | Same as C3 | Missing local assignment identity fails closed | On | Pass | 28 / 24 | 11 | 1s |
| `c4` | 3 / 1 / 1 | 1 primary crash; async WAL durability | Off | Expected `NoAckedLoss` violation | 32 / 21 | 9 | 1s |
| `l1` | 2 / 1 / 0 | 1 recovery; no faults; weak fairness; no constraint/symmetry | Both fixes | Safety and all liveness properties pass | 20 / 17 | 15 | 1s |
| `l2` | 2 / 1 / 0 | Up to 2 recovery attempts; exactly 1 transient target crash/restart; weak fairness; no constraint/symmetry | Both fixes | Safety and all liveness properties pass | 167 / 98 | 21 | 2s |
| `fixed-crash` | 3 / 1 / 2 | Full `Next`; 1 crash/recovery; message loss/delay; term 3; log 2; view lag 1 | Both fixes | Pass | 78,085,145 / 11,366,697 | 41 | 12m18s |
| `fixed-partition` | 3 / 1 / 2 | Full `Next`; 1 live-node partition/recovery; message loss/delay; term 3; log 2; view lag 1 | Both fixes | Pass | 97,142,840 / 12,887,671 | 42 | 14m47s |

The two long fixed-design configurations use the complete `Next` relation, not
a scenario wrapper. They constrain writes to `Put` and disable optional
recovery setup/cancellation/expiry injection, while retaining every write,
replication, message-loss/delay, recovery-progress, Raft, view-delivery,
crash/restart, or live-suspicion interleaving within the numeric bounds.

The larger `fixed-simulation` profile uses 3 nodes, 2 documents, 4 writes,
2 crashes, 1 partition, 2 recoveries, term 4, 3 in-flight messages, view lag 4,
and 8 Raft entries. With seed `20260926`, depth 80, and 10,000 requested traces,
TLC checked 1,574,980 states in 1m44s without finding a violation. Simulation
is sampling, not exhaustive model checking.

The C2 configurations deliberately use canonical role constants rather than
symmetry reduction. Their next-state relation is restricted to the
partition-promotion-activation-write path needed to keep the living regression
well below the CI budget.

## Counterexamples

- [C1 same-node allocation ABA](traces/C1-allocation-aba-no-partial-serve.md)
- [C2 stale-primary duplicate sequence](traces/C2-stale-primary-unique-seq.md)
- [Why the replica fence must survive restart](traces/Fence-volatile-restart-stale-probe.md)
- [Retired global-term stale-apply property](traces/Fixed-partition-prepromotion-inflight-apply.md)
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
