# Proposed acknowledgement-policy controls and witnesses

**Status:** Bounded design regressions, not historical Rust vulnerabilities.
Produced on October 5, 2026 with Java 25 and checksum-pinned TLA+ tools 1.7.4.
Each `.cfg` retains its named unsafe, rejected-alternative, or positive-witness
verdict in the fast runner. Raw TLC logs are session evidence; rerun the named
configurations to regenerate their finite traces.

The module reuses D1 operation/planner/checkpoint state and Raft allocation
transitions. Unsafe controls use three nodes and two writes. Availability
witnesses use one write; the fail-stop witness uses two. These are the declared
finite constants, not arbitrary clusters or an implementation proof.

## Exclusion before commit

`d2-early-exclusion` violates `D2ExclusionCommitted`.

1. The primary durably applies a write and sends it to both required replicas.
2. One request times out without that replica applying it.
3. The unsafe alternative treats the pending exclusion as already effective.
4. The other replica confirms, and the primary acknowledges before the
   conditional removal commits.

The failed allocation is still authoritative. Queuing an exclusion or waiting
out a transport timer is not confirmation of committed membership.

## Forgotten exclusion debt

`d2-no-sticky` violates `D2NoAckWithDebt`.

1. Metadata becomes unavailable, and the first write loses a required reply.
2. The exclusion cannot settle; the first client result is indeterminate.
3. A later write reaches the copies and obtains operation-level confirmations.
4. The unsafe alternative acknowledges it despite unresolved debt in that
   primary authority epoch.

A client deadline can release request resources, not erase the unresolved
membership decision or reopen the old permit.

## Minimum-copy bypass

`d2-minimum-bypass` violates `D2MinimumCopies`.

Both replicas fail and are committed out. Only the primary remains in the
certificate. Ignoring the configured floor of two allows an acknowledgement
with one eligible durable copy. Successful removal alone does not satisfy
the independently configured redundancy requirement.

## Old writer continues after rejection

`d2-no-self-fence` violates `D2NoPostFenceAdmission`.

1. The old primary queues a term-1 replica exclusion.
2. Another metadata leader commits promotion while the old applied view lags.
3. The queued exclusion is committed as rejected under the new authority.
4. The old source observes rejection but the unsafe alternative ignores its
   locally revoked permit and appends another old-term write.

The selected `d2-stale-self-fence` relation rejects that admission without WAL
mutation. This models the future D5 local policy; it does not claim that the
current Rust handler already self-fences on this response.

## Prefix-gated head-of-line blocking

`d2-gap-prefix` retains a temporal counterexample for `D2GapProgress`.

The first operation's replica messages are held. A later delete is applied
and durable on both replicas, whose contiguous prefixes remain below it.
The rejected prefix-gated alternative never acknowledges the delete, even
under the declared fair delivery and response actions. The selected
`d2-gap-operation` configuration progresses while retaining the gap and WAL
history required for replay; no checkpoint skips the missing earlier position.

## Positive reachability witnesses

These checks deliberately violate a negated target while retaining the selected
policy's safety invariants. They show reachable behavior, not universal liveness.

| Runner | Retained behavior | Owning model actions |
| --- | --- | --- |
| `d2-quorum-loss-witness` | Metadata quorum is unavailable at ACK time, but all three copies prove the exact durable operation and the primary acknowledges without exclusion debt. | `D2LoseMetadata`, D1 apply/reply actions, `D2Acknowledge` captures `ackProof.metadataDown`. |
| `d2-exclusion-witness` | Both failed allocations are committed out and observed; the remaining primary acknowledges under the default minimum of one. | `D2ReplicaFailure`, `D2CommitRemoval`, `D2ObserveRemoval`, `D2Acknowledge`. |
| `d2-fail-stop-witness` | Abstract post-WAL failure makes the first outcome indeterminate and revokes the local permit. The next request is not executed and reaches no WAL. | `D2PostWalFailure`, `D2NotExecuted`. |

The paired minimum-bypass control still rejects a one-copy certificate under
the configured floor of two. The witness does not make one copy redundant.

`d2-quorum-loss-negative` checks the witness itself: its counterfactual relation
forbids ACK while metadata is down. The negated witness must remain true even
when an outage follows a healthy ACK. The retained positive trace orders
`D2LoseMetadata` before `D2Acknowledge`; the earlier ACK-then-outage trace was an
evidence bug, not proof of quorum-independent acknowledgement.
