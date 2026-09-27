# FerrisSearch shard replication and peer-recovery model

This directory contains a bounded TLA+ model of one FerrisSearch
`local_shards` shard plus a minimal two-shard control-plane isolation model.
The model began at source baseline `8f17172` and now includes the allocation,
fencing, empty-store, copy-failure, and pending-restart protocol refinements on
this branch.

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
./scripts/tla/check.sh c1-aba-fixed c2-fixed g1-empty-store g2-liveness
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
liveness configurations use neither symmetry nor a state constraint.

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
| `MC_L1_Bump.tla` | Review-derived settlement-deadline term-bump liveness check. |
| `MC_L2_PrimaryRestart.tla` | Pending-target resolution after source-primary crash, restart, and re-activation. |
| `MC_L2_PrimaryRestart_IdleShard.tla` | Reviewer-named idle-shard check driven only by proactive lifecycle activation. |
| `MC_L2_PrimaryRestart_NoTrigger.tla` | Historical liveness variant without fairness on proactive activation. |
| `MC_L2_Promotion.tla` | Pending-target resolution after another in-sync replica is promoted. |
| `MC_PendingRestart.tla` | Durable pending-marker restoration and historical restarted-target wipe regression. |
| `MC_StorageFailure.tla` | Corruption, persistent-I/O escalation, promote-only copy failure, and post-failure write liveness. |
| `MC_TwoShardIsolation.tla` | Minimal index-level check that one red shard does not block failover and allocation on a sibling shard. |
| `MC_FenceDurability.tla` | Bounded check that a learned replica fence must survive restart. |
| `MC_G1_EmptyStore.tla` | CreateIndex, permitted initial empty-copy creation, pre-activation disk loss, first activation, and first acknowledged write. |
| `MC_G2_CopyFailure.tla` | Replica/primary copy-failure reporting, exact-allocation removal, promotion or red state, fresh allocation, and recovery safety. |
| `MC_G2_Liveness.tla` | Fair replica disk-loss reporting, stale-report rejection, resumed writes, and replacement recovery. |
| `MC_Fixed_Crash.cfg`, `MC_Fixed_Partition.cfg` | Unrestricted three-voter fixed-design safety checks with two writes, recovery, message loss/delay, and crash or partition faults. |
| `MC_Fixed_Simulation.cfg` | Larger seeded simulation profile for deeper randomized executions. |

## Core abstractions

- The full replication/recovery model contains one shard and at most one local
  copy per node. `MC_TwoShardIsolation.tla` is a separate minimal
  control-plane slice for cross-shard `UpdateIndex` validation.
- Documents are represented by bounded document keys and unique write IDs.
  Deletes are distinct write kinds; value/rollback checks use operation
  identity and exact sequence numbers.
- At most one client write is active at once. That write still interleaves with
  Raft, replication, recovery, crashes, view delivery, and message loss.
- `InitialInitialized = TRUE` starts at normalized term 1 after initial
  activation, as the original configurations did. `FALSE` starts at the
  committed CreateIndex routing record with no open copies or in-sync
  replicas; the first activation is modeled explicitly.
- Routing carries a monotonic `initialized` bit. A cleared primary allocation
  represents a red shard: no primary copy is authoritative and client writes
  fail closed.
- Raft requires a live connected leader and a live majority of current voters.
  The leader applies a committed command before its synchronous write returns.
  Other nodes nondeterministically apply a committed-log prefix.
- Per-node views are monotonic prefixes of one durable Raft log. Process
  restart may replay a lagging prefix but never moves a node's view backward.
  Losing `raft.db` and rejoining under the same node name is outside the model.
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
- Persistent storage I/O retry counts and elapsed time are abstracted as a
  nondeterministic `StorageRetrying` to `StorageFailed` escalation. Weak
  fairness on that action represents exhaustion of the finite retry budget.
- `FailShardCopy` omits index name, UUID, and shard ID from its abstract record
  because the model contains exactly one fixed-UUID shard. Allocation identity,
  conditional commit, promotion, unassignment, and view lag remain explicit.

## Action-to-code mapping

| Model action | Rust boundary represented |
| --- | --- |
| `ClientWrite` | Coordinator routing in `src/api/index/`. |
| `PrimaryAccept`, `PrimaryReject`, `PrimaryAck`, `PrimaryFail` | `TransportService::{index_doc,bulk_index,delete_doc}`, including `ensure_primary_activated`, `peer_recovery_write_guard`, `validated_primary_write_state`, and all-in-sync acknowledgement. |
| `ReplicaApply`, `DeliverReplicaAck` | `TransportService::{replicate_doc,replicate_bulk}` and `replication::{replicate_write,replicate_bulk}`. |
| `ReplicaReject`, `DeliverReplicaNack` | Pre-WAL identity/fence rejection in the same replica handlers and propagation as a synchronous replication failure. |
| `ProposeActivate`, `ObserveActivation`, `CancelActivation` | `TransportService::ensure_primary_activated`. |
| `CommitRaft` | `ClusterStateMachine::apply_command`, including allocation-matched `ActivatePrimary` and `FailShardCopy`; rejected conditional commands retain a log position without changing routing. |
| `DeliverView` | Per-node `ClusterManager` observation of an applied Raft prefix. |
| `ElectLeader` | openraft leader election, abstracted to a live connected voter with quorum. |
| `SuspectAndRemove`, `ObserveRoutingAccepted`, `ObserveRoutingRejected` | Leader dead-node loop in `src/node/mod.rs`, `IndexMetadata::{remove_node,select_promotion_candidate,promote_replica_to}`, and checked `UpdateIndex`. |
| `ChangeRaftMembership`, `ProposeRemoveNode`, `ObserveNodeRemoved` | `Raft::change_membership`, followed by `ClusterCommand::RemoveNode`; removal is deferred after rejected routing updates. |
| `Rejoin`, `ObserveRejoin` | Follower `JoinCluster` retry and committed `ClusterCommand::AddNode`. |
| `AllocateAfterLifecycle`, `ObserveAllocationAccepted`, `ObserveAllocationRejected` | `IndexMetadata::allocate_unassigned_replicas` and the allocator phase of the leader lifecycle loop. |
| `ReportShardCopyFailure` | `open_local_assigned_shards`, `TransportService::fail_shard_copy`, and `TransportClient::forward_fail_shard_copy`. |
| `CorruptShardStorage`, `BeginPersistentStorageFailure`, `EscalatePersistentStorageFailure` | Definitive storage decoding/validation failure and bounded persistent-I/O retry escalation in shard open/reconciliation. |
| `LifecycleProposeActivation` | Proactive local-primary activation from the node lifecycle after startup or promotion. |
| `StartRecovery`, `SourceSetupFailure`, `PollSetupFailure` | `run_peer_recovery`, `start_peer_recovery_inner`, `launch_source_setup`, and `source_start_status`. |
| `SourceSnapshot` | `HotEngine::prepare_peer_recovery_snapshot`, including commit, durable checkpoint, hard-linked files, and `register_retention_pin`. |
| `TargetBeginInstall`, `InstallSnapshot` | `ShardManager::{begin_peer_recovery_target_blocking,prepare_peer_recovery_target_blocking,finalize_peer_recovery_target_blocking}`. |
| `FetchOps`, `ApplyOps`, `FinishCatchUp` | `fetch_recovery_ops_inner` and `apply_recovery_operations`. |
| `BeginPrepareFinalize`, `CancelPrepareFinalize`, `AcquireFinalizeBarrier`, `FinishFinalizeTail` | `prepare_finalize_recovery_inner` and `FinalizePreparingGuard`. |
| `TargetComplete` | `mark_peer_recovery_awaiting_membership_blocking`. |
| `BeginSettlement`, `ProposeMarkInSync`, `SettlementDeadline`, `ObserveAdmission` | `complete_finalize_recovery_inner`, `settle_peer_recovery`, `submit_mark_replica_in_sync`, `submit_settlement_term_bump`, and `observe_membership`. |
| `TargetObserveAdmitted`, `TargetObserveRejected` | `observe_target_membership` and `PeerRecoveryDriver::reconcile_pending_targets`. |
| `RestorePendingMarker` | Restart reconciliation loading a matching durable awaiting-membership marker before recovery candidate selection. |
| `PRLegacyWipeAndDelayedAdmission` | Historical B3 scenario wrapper compressing target reattach/wipe plus the already-queued admission application. |
| `AbortSession`, `ExpireSession`, `ExpireFinalizeWithoutMark` | `abort_shard_session`, `reap_expired_sessions`, `reap_peer_recovery_sessions`, and `settle_abandoned_finalize`. |
| `Flush` | `HotEngine::flush_with_global_checkpoint` and `HotTranslog::{truncate,truncate_below}`. |
| `Crash`, `Restart` | Process loss/restart and WAL reopen/replay; volatile activation, barriers, and source sessions are discarded while durable pending markers survive. |
| `PartitionMetadata`, `LoseMsg` | Metadata-link isolation and delayed/lost transport requests. |
| `DiskLoss`, `OpenAssignedEmptyCopy` | Same-node missing shard directory and `open_local_assigned_shards` / `ShardManager::open_assigned_shard_with_settings`; only a pre-activation CreateIndex allocation may be created empty in the fixed design. |

## Allocation-ID variant

`AllocationIds = FALSE` models the historical pre-fix Rust behavior. Replica
authority was keyed by node name, index UUID, primary, and primary term.

`AllocationIds = TRUE` models the implemented allocation-identity protocol:

1. every routed copy has a durable assignment ID;
2. removal clears that assignment;
3. reallocation uses a fresh monotonic ID derived from the Raft position;
4. the target sends the allocation ID from its own local view in
   `StartRecovery`;
5. the source rejects start until its current allocation ID exactly matches;
6. snapshot setup, the source session, the target pending marker, and
   `MarkReplicaInSync` retain that ID;
7. the state machine admits only an exact current allocation-ID match; and
8. target observation admits the same ID when in sync or admits promotion;
   otherwise it rejects a missing/different allocation, a strictly newer
   applied term, or a different applied primary, and returns `Unknown` only
   while the same primary, term, and allocation remain possible.

The Rust implementation uses this complete handshake. Adding only an allocation
field to `MarkReplicaInSync` is insufficient: the retained
[`stale-start trace`](traces/C1-allocation-id-stale-start-no-partial-serve.md)
shows why target and source must agree before snapshot setup.

## Replica-fencing variant

`ReplicaFencing = FALSE` models the historical pre-fix replica handlers, which
accepted primary-originated operations without checking the sender's primary
term.

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

## Empty-store and copy-failure rules

G1 models CreateIndex routing with `initialized = FALSE`, allocation ID 1 for
each initial assignment, no in-sync replicas, and no local copies. The model
over-approximates Rust by allowing any initial assignment to create an empty
local copy while its applied view remains uninitialized. Rust uses the stricter
rule that only the initial primary allocation may do so; initial replicas are
always populated by peer recovery. `ActivatePrimary` carries the primary
allocation ID; an exact-match commit monotonically sets `initialized = TRUE`.
No write can be acknowledged before that transition.
Initial replicas are not authoritative copies: a new index is single-copy
until peer recovery admits them. Setting `max_concurrent_peer_recoveries = 0`
therefore leaves new indices single-copy.

G2 models `FailShardCopy(node, allocation_id)` as a conditional Raft command:

- an authoritative primary or in-sync replica with missing/malformed/mismatched
  identity reports its target-observed allocation ID;
- a failed recovery install may also report, while an ordinary newly assigned
  out-of-sync recovery target does not;
- an exact replica match removes it from `replicas` and `inSync`, clears its
  allocation, and increments `unassigned`;
- an exact primary match applies only when an in-sync replica can be promoted
  with a term increment; without a survivor, the command is rejected and
  cannot turn the shard red;
- a stale allocation ID commits as a rejected command with unchanged routing;
  and
- the allocator requires a live allocated primary, assigns a fresh ID, and
  peer recovery installs and admits the replacement.

## Persistent storage failures

`FaultMode = "S1"` adds two live-copy failure classes:

- corruption or decode/validation failure enters `StorageFailed`
  immediately; and
- persistent I/O enters `StorageRetrying`, where open attempts remain blocked,
  before a separate escalation action reaches `StorageFailed`.

Both states are persistent across process restart. Only `StorageFailed` is
reportable. Replica reports remove the exact allocation, after which writes no
longer wait for that copy. Primary reports are promote-only: an exact report
with an in-sync candidate promotes it and preserves every acknowledged write;
without a candidate the report commits as rejected and routing remains
unchanged.

## Pending-target restart and observation

The durable awaiting-membership marker is represented by
`pendingPrimary`, `pendingTerm`, and `pendingAllocation`; `copyMode =
"Pending"` is volatile runtime registration. A target crash clears that
runtime registration but preserves the marker. With
`RestorePendingOnRestart = TRUE`, `RestorePendingMarker` recreates runtime
pending state only when the fixed UUID, durable copy allocation, and target
view allocation all match. A matching marker prevents both a new recovery
start and destructive target preparation.

Target observation is ordered:

1. `Admitted` if the same allocation is in sync, or if the target has been
   promoted;
2. otherwise `Rejected` if the target allocation is missing/different, the
   applied term is strictly newer than the marker term, or the applied primary
   differs from the marker primary;
3. otherwise `Unknown`.

This relies on monotonic applied Raft views: if admission committed before a
later term bump or primary change, the same ordered view already contains the
admission and the first rule wins. A red view with the same primary, term, and
target allocation remains `Unknown`.

### Required Rust contract

The combined Rust implementation must follow the model variant as one protocol:

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
9. Pending-target observation first admits the same allocation when it is in
   sync, or admits the target after promotion. Otherwise it rejects a missing
   or different allocation, a strictly newer applied term, or a different
   applied primary. It returns `Unknown` only while the same primary, term, and
   allocation remain possible; a red view with that same primary and term is
   therefore still unknown.
10. Restart restores runtime pending state from the durable marker only when
    the marker's UUID and allocation match the durable copy and current
    assignment. A matching marker blocks `StartRecovery` and destructive
    target preparation until authoritative admission or rejection clears it.
11. Any rejection is returned through the existing synchronous replication
    failure path; it must not be converted into a successful item or request.
12. CreateIndex routing starts with `initialized = false`. Rust may create an
    empty copy only for the initial primary allocation before first activation;
    all initial replicas remain out of sync until recovery. The model's broader
    initial-copy action is a safety over-approximation of this Rust rule.
13. `ActivatePrimary` carries and conditionally checks the primary allocation
    ID. Its first successful application sets `initialized = true`
    monotonically. A missing or mismatched primary copy cannot activate.
14. After initialization, a missing, malformed, or mismatched authoritative
    copy never creates an empty engine and never serves. A fresh out-of-sync
    replica is populated only by verified peer-recovery install.
15. Corruption-class storage decode or validation failures are definitive.
    Persistent I/O remains under per-copy retry/backoff until its bounded
    count/time budget is exhausted. A failed replica then reports index name,
    UUID, shard ID, node ID, and allocation ID. A failed primary reports only
    for promote-only handling when an in-sync candidate exists.
16. `FailShardCopy` changes routing only on an exact allocation match. Replica
    failure removes it from `replicas` and `inSync` and increments
    `unassigned`. Primary failure promotes an in-sync copy with a term bump;
    without a survivor, the command is rejected and never clears the primary
    allocation or turns the shard red.
17. Allocation after copy failure uses a fresh ID and requires a surviving
    allocated primary. The replacement remains out of sync until recovery
    installs matching durable identity and admission commits.
18. Node lifecycle proactively invokes primary activation after startup or
    promotion whenever the node's applied view names it as primary and the
    current incarnation has not activated that term. Pending-target progress
    must not depend on a later client write or recovery request.

The model updates fence and accepted operation atomically. Rust may persist the
fence immediately before the WAL mutation; a crash in between is conservative
because it can reject more old-term traffic but cannot acknowledge a mutation
without its fence.

## Properties

Safety invariants:

- `RoutingWellFormed`
- `InitializationMonotonic`
- `InitializationBeforeAcknowledgement`
- `StaleFailShardCopyRejected`
- `RedShardRejectsWrites`
- `PendingMarkerProtectsCopy`
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
- `PendingMarkerResolves`
- `PreActivationDiskLossHarmless`
- `WritesResumeAfterReplicaDiskLoss`
- `ReplacementEventuallyInSync`
- `StaleFailureEventuallyRejected`
- `FailedReplicaRemoved`
- `FailedPrimaryReplaced`
- `WritesResumeAfterStorageFailure`
- `PrimaryReportEventuallyRejected`

`RecoveryConverges` is attempt-level: an assigned recovery candidate
eventually becomes in sync, becomes primary, or reaches definitive rejection
and an install marker. It does not by itself prove that an arbitrary sequence
of retries eventually admits the same routing assignment.

`MC_L1.tla` covers one fault-free, term-1 recovery attempt. Under weak fairness
for every phase, commit, and view/target observation, the barrier releases and
the attempt reaches admission, promotion, or definitive rejection.
`MC_L1_Bump.tla` adds the settlement deadline at `MaxTerm = 2`; the old
observation rule fails, while the corrected rule rejects and clears the stale
pending marker. `MC_L2.tla` adds one target crash/restart at any recovery phase,
restoration of a matching durable pending marker, and permanent fault
cessation; it does not model failure-detector removal.

`MC_L2_PrimaryRestart.tla` forces a pending target through source-primary
crash, restart, election, and allocation-matched re-activation to term 2.
The weak fairness is attached to `LifecycleProposeActivation`, the proactive
node-lifecycle trigger, rather than to client writes or recovery requests.
`MC_L2_PrimaryRestart_NoTrigger.tla` omits only that fairness and retains the
reviewer's idle-shard stutter counterexample.
`MC_L2_Promotion.tla` uses three voters and forces a different in-sync replica
to become primary at term 2. Both check that the old pending marker reaches
admission or definitive rejection. `MC_PendingRestart.tla` separately checks
restart during unresolved settlement, including the historical no-restore
wipe. None of these liveness configurations uses symmetry reduction or a
state constraint.

`MC_G1_EmptyStore.tla` weakly fairly schedules initial copy creation, one
pre-activation crash/disk loss/restart, allocation-matched first activation,
and the first write. `MC_G2_Liveness.tla` weakly fairly schedules one in-sync
replica crash/disk loss/restart, permanent fault cessation, exact failure
report, fresh allocation, a delayed stale report and its rejection, a resumed
write, every recovery phase, Raft commits, and target view delivery. Neither
uses symmetry reduction or a state constraint.

`MC_StorageFailure.tla` first acknowledges one write, then nondeterministically
injects corruption or persistent I/O. Fair retry escalation and failure
reporting remove a failed replica or promote an in-sync replacement primary;
the second write must eventually acknowledge. A separate two-node
configuration proves that a primary report without an in-sync candidate is
rejected and does not clear the primary allocation.

## Configurations and results

Results below were produced on September 27, 2026 with Java 25 and the pinned
TLA+ tools jar. Times are TLC wall times on one development host, not
performance benchmarks.

| Runner name | Nodes/docs/writes | Fault and protocol bounds | Allocation IDs | Expected/result | Generated / distinct | Depth | Time |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| `c1-fast` | 2 / 1 / 1 | 1 crash; no recovery; term 3; log 5; view lag 2 | Off | Pass | 3,538 / 999 | 15 | 3s |
| `c1-recovery` | 2 / 1 / 1 | 1 recovery; no crash; term 3; log 3; view lag 2 | Off | Pass | 26,133 / 6,734 | 29 | 4s |
| `c1-aba` | 3 / 1 / 0 | 1 recovery and crash; term 2; log 5; view lag 5 | Off | Expected `NoPartialServe` violation | 606,551 / 182,823 | 29 | 17s |
| `c1-aba-fixed` | 3 / 1 / 0 | 1 recovery and crash; term 2; log 6; view lag 5 | Allocation IDs + durable fencing | Pass | 2,976,559 / 810,897 | 45 | 1m14s |
| `c2` | 3 / 1 / 2 | 1 metadata partition; term 3; log 2; canonical primary/leader | Off | Expected `C2RejectsStaleMessage` violation | 19 / 16 | 14 | 1s |
| `c2-allocation-ids` | Same as C2 | Same as C2 | Allocation IDs only | Expected `C2RejectsStaleMessage` violation | 16 / 16 | 14 | 1s |
| `c2-fixed` | Same as C2 | Same as C2 | Allocation IDs + durable fencing | Pass | 28 / 20 | 16 | 2s |
| `fence-volatile` | 3 / 1 / 2 | 1 partition and replica crash/restart; term 3 | Allocation IDs + volatile fencing | Expected `FenceRejectsStaleProbe` violation | 17 / 17 | 15 | 1s |
| `fence-durable` | Same as volatile-fence check | Same schedule | Allocation IDs + durable fencing | Pass | 21 / 19 | 16 | 1s |
| `c3` | 3 / 1 / 1 | 1 crash and disk loss; no metadata update | Off | Expected `NoAckedLoss` violation | 36 / 25 | 12 | 1s |
| `c3-allocation-ids` | Same as C3 | Missing local assignment identity fails closed | On | Pass | 28 / 24 | 11 | 1s |
| `c4` | 3 / 1 / 1 | 1 primary crash; async WAL durability | Off | Expected `NoAckedLoss` violation | 32 / 21 | 9 | 1s |
| `g1-empty-store` | 2 / 1 / 1 | CreateIndex; pre-activation crash/disk loss/restart; first activation | Both fixes + G1 | Safety and liveness pass | 14 / 14 | 13 | 1s |
| `g2-replica` | 3 / 1 / 1 | In-sync replica disk loss; exact failure report; fresh allocation/recovery | Both fixes + G2 | Pass | 110,742 / 34,457 | 46 | 4s |
| `g2-primary` | 3 / 1 / 1 | Primary disk loss; in-sync promotion; fresh allocation/recovery | Both fixes + G2 | Pass | 49,506 / 17,863 | 46 | 4s |
| `g2-primary-no-replica` | 2 / 1 / 1 | Primary disk loss with no in-sync survivor | Both fixes + G2 | Report rejected; primary allocation retained | 23 / 20 | 13 | 1s |
| `g2-liveness` | 2 / 1 / 1 | Replica disk loss; faults stop; stale report; write and recovery fairness | Both fixes + G2 | Safety and all liveness properties pass | 433 / 184 | 32 | 3s |
| `pending-restart-legacy` | 2 / 1 / 1 | Pending target restarts; marker ignored; reattach/wipe plus delayed admission | Historical pending behavior | Expected `NoPartialServe` violation | 20 / 19 | 18 | 1s |
| `pending-restart-fixed` | Same as legacy | Matching marker restored; admission may race restoration | Marker restoration enabled | Safety and liveness pass | 81 / 49 | 22 | 2s |
| `l1` | 2 / 1 / 0 | 1 recovery; no faults; weak fairness; no constraint/symmetry | Both fixes | Safety and all liveness properties pass | 20 / 17 | 15 | 1s |
| `l1-bump` | 2 / 1 / 0 | Settlement deadline; `MaxTerm = 2`; weak fairness | Corrected observation | Safety and liveness pass | 79 / 50 | 18 | 2s |
| `l2-primary-no-trigger` | 2 / 1 / 0 | Pending target; source restart; no lifecycle-activation fairness | Historical activation behavior | Expected temporal violation | 94 / 49 | 16-state lasso | 2s |
| `l2-primary-idle` | 2 / 1 / 0 | Pending target; idle source restart/election; lifecycle activation to term 2 | Lifecycle trigger enabled | Safety and liveness pass | 94 / 49 | 22 | 2s |
| `l2-promotion` | 3 / 1 / 0 | Pending target; distinct in-sync replica promoted to term 2 | Corrected observation | Safety and liveness pass | 43 / 29 | 19 | 2s |
| `l2` | 2 / 1 / 0 | Up to 2 recovery attempts; exactly 1 transient target crash/restart; marker restoration; weak fairness | Both fixes | Safety and liveness pass | 200 / 112 | 21 | 2s |
| `storage-replica` | 3 / 1 / 2 | Corruption or persistent-I/O escalation on an in-sync replica | Promote-only storage rules | Replica removed; second write succeeds | 33 / 29 | 17 | 1s |
| `storage-primary` | 3 / 1 / 2 | Corruption or persistent-I/O escalation on primary with two in-sync replicas | Promote-only storage rules | Candidate promoted; second write succeeds | 39 / 35 | 20 | 2s |
| `storage-primary-no-replica` | 2 / 1 / 1 | Failed primary with no in-sync candidate | Promote-only storage rules | Failure report rejected; routing retained | 11 / 11 | 8 | 1s |
| `two-shard` | 3 nodes / 2 shards | One shard red; sibling primary failure, promotion, and allocation | Per-shard update validation | Safety and liveness pass | 4 / 4 | 4 | 1s |
| `fixed-crash` | 3 / 1 / 2 | Full `Next`; 1 crash/recovery; message loss/delay; term 3; log 2; view lag 1 | Full fixed design | Pass | 87,012,150 / 12,495,758 | 42 | 19m05s |
| `fixed-partition` | 3 / 1 / 2 | Full `Next`; 1 live-node partition/recovery; message loss/delay; term 3; log 2; view lag 1 | Full fixed design | Pass | 99,132,329 / 13,133,936 | 43 | 21m39s |

The two long fixed-design configurations use the complete `Next` relation, not
a scenario wrapper. They constrain writes to `Put` and disable optional
recovery setup/cancellation/expiry injection, while retaining every write,
replication, message-loss/delay, recovery-progress, Raft, view-delivery,
crash/restart, or live-suspicion interleaving within the numeric bounds.

The larger `fixed-simulation` profile uses 3 nodes, 2 documents, 4 writes,
2 crashes, 1 partition, 2 recoveries, term 4, 3 in-flight messages, view lag 4,
and 8 Raft entries. With seed `20260926`, depth 80, and 10,000 requested traces,
TLC checked 1,588,868 states in 2m49s without finding a violation. Simulation
is sampling, not exhaustive model checking.

`MC_TwoShardIsolation.tla` is deliberately smaller than the one-shard
data-plane model. It models index-level validation, one red shard, sibling
failover, and sibling allocation; it does not duplicate write, WAL, recovery,
or message state for two shards. Its result is evidence specifically against
the B1 cross-shard validation freeze, not a two-shard replication proof.

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
- [B2 settlement-deadline pending target](traces/B2-settlement-deadline-pending-unknown.md)
- [B3 restarted pending target wipe](traces/B3-pending-restart-wipe.md)
- [R2 idle primary without lifecycle activation](traces/R2-idle-primary-no-activation.md)

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
- The persistent-I/O retry count, elapsed-time threshold, and backoff duration
  are abstracted as one nondeterministic escalation step; their concrete
  numeric policy is not verified here.
- Applied Raft views are monotonic. Loss of a node's durable `raft.db` followed
  by same-name rejoin is outside the model and requires separate identity and
  bootstrap handling.
