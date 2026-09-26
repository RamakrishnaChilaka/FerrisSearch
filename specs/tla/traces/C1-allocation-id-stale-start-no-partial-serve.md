# Allocation-ID variant counterexample: stale target start

**Date:** September 26, 2026  
**Configuration:** `MC_C1_ABA_fixed.cfg`  
**TLC result:** unexpected violation of `NoPartialServe`  
**Raw trace:** [`C1-allocation-id-stale-start-no-partial-serve.log`](C1-allocation-id-stale-start-no-partial-serve.log)  
**Raw trace SHA-256:** `d08062d6d6b9994247b175ab9970b4d31aedf0dabbe8957be0b8a9dc0aa51d92`

This was an expected-pass configuration, so work stopped at this trace. The
model was not weakened and Rust source was not changed.

## Trace

| TLC state | Model action | Meaning |
| --- | --- | --- |
| 1-15 | Crash, ordered removal, committed `AddNode`, allocation | `n3` is removed and re-added. The new committed replica assignment has allocation ID 5. The target's lagging local view still contains the original allocation ID 1. |
| 16 | `StartRecovery(n3, n1)` | The model lets the source bind the session to allocation ID 5 even though the target initiated recovery from a view containing ID 1. |
| 17-22 | Snapshot, install, catch-up, exclusive finalize | Recovery completes against source allocation ID 5 while the target's local metadata remains at allocation ID 1. |
| 23-24 | `TargetComplete`; `BeginSettlement` | The persistent pending marker records allocation ID 5. |
| 25-26 | `ProposeMarkInSync`; committed `MarkReplicaInSync` | The command carries allocation ID 5 and is correctly accepted by the allocation-aware state machine. |
| 27 | `TargetObserveRejected` | The target compares pending ID 5 with its stale local ID 1, treats the mismatch as definitive, and writes the destructive install marker even though committed metadata has admitted allocation 5. `NoPartialServe` fails. |

## Assessment

This is a **model-variant error**, not evidence that allocation IDs are
insufficient. The proposed handshake was modeled only at the source:
`StartRecovery` copied the allocation ID from the source's view. It did not
require the target's request to carry the allocation ID from the target's own
view and did not require an exact source/target match before snapshot setup.

The corrected variant must model the start request as
`StartRecovery(target, source, targetAllocationId)` and require:

1. the target's local assigned allocation ID is nonzero;
2. the request carries that target-observed ID; and
3. the source's current assigned ID exactly matches it.

With that handshake, this trace cannot start until `n3` has applied the new
allocation ID 5. Because applied views do not regress, the later local
observation cannot mistake the old ID 1 for a newer conflicting assignment.
A subsequent missing or changed ID remains a definitive rejection.
