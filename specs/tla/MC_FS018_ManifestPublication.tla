------------------- MODULE MC_FS018_ManifestPublication -------------------
\* Bounded model of the ADR 0002 / FS-017..FS-019 fenced single-sequencer
\* manifest publication protocol over conditional object storage and Raft
\* writer epochs.

EXTENDS Naturals, FiniteSets

CONSTANTS Variant

VARIABLE p

Ops == 1..2
Fingerprints == 1..2
Epochs == 1..2
Gens == 1..2
Revs == 0..5
Variants == {
    "Safe",
    "UnfencedActivate",
    "BlindOverwrite",
    "UncheckedFingerprintRetry"
}

ManifestRecordType ==
    [gen : Gens,
     op : Ops,
     parentGen : 0..2,
     parentOp : 0..2,
     epoch : Epochs,
     splits : SUBSET Ops]

FS018Init ==
    /\ Variant \in Variants
    /\ p =
        [raftPhase |-> "Unassigned",
         raftEpoch |-> 0,
         raftFencedRev |-> 0,
         maxActivatedEpoch |-> 0,
         maxFencedEpoch |-> 0,
         ptrRev |-> 0,
         ptrEpoch |-> 0,
         ptrGen |-> 0,
         ptrOp |-> 0,
         ptrSplits |-> {},
         everCommittedSplits |-> {},
         staleEpochCommits |-> {},
         intentFp |-> [op \in Ops |-> 0],
         acceptedFps |-> [op \in Ops |-> {}],
         prepared |-> {},
         committed |-> {},
         manifests |-> {},
         attemptOp |-> [e \in Epochs |-> 0],
         attemptParentRev |-> [e \in Epochs |-> 0],
         attemptParentGen |-> [e \in Epochs |-> 0],
         attemptParentOp |-> [e \in Epochs |-> 0],
         attemptParentSplits |-> [e \in Epochs |-> {}],
         attemptGen |-> [e \in Epochs |-> 0]]

\* Producer writes immutable publications/<op>/intent.json (or retries with same/different fingerprint).
FS018StageIntent ==
    \E op \in Ops, fp \in Fingerprints :
        \/ /\ p.intentFp[op] = 0
           /\ p' = [p EXCEPT
                !.intentFp[op] = fp,
                !.acceptedFps[op] = @ \cup {fp}]
        \/ /\ p.intentFp[op] # 0
           /\ p.intentFp[op] # fp
           /\ Variant = "UncheckedFingerprintRetry"
           /\ p' = [p EXCEPT
                !.intentFp[op] = fp,
                !.acceptedFps[op] = @ \cup {fp}]

\* Producer uploads deterministic bundle and writes publications/<op>/prepared.json.
FS018PrepareBundle ==
    \E op \in Ops :
        /\ p.intentFp[op] # 0
        /\ op \notin p.prepared
        /\ p' = [p EXCEPT !.prepared = @ \cup {op}]

\* Raft allocates a strictly greater writer_epoch in Fencing phase.
FS018AcquireEpoch ==
    /\ p.raftEpoch < 2
    /\ LET nextEpoch == p.raftEpoch + 1
       IN p' = [p EXCEPT
            !.raftPhase = "Fencing",
            !.raftEpoch = nextEpoch]

\* Candidate sequencer fences manifest.current.json with its writer_epoch before activation.
FS018FencePointer ==
    /\ p.raftPhase = "Fencing"
    /\ p.ptrRev < 5
    /\ p.ptrEpoch < p.raftEpoch
    /\ LET nextRev == p.ptrRev + 1
       IN p' = [p EXCEPT
            !.ptrRev = nextRev,
            !.ptrEpoch = p.raftEpoch,
            !.maxFencedEpoch =
                IF p.raftEpoch > p.maxFencedEpoch
                THEN p.raftEpoch
                ELSE p.maxFencedEpoch]

\* Candidate sequencer activates its lease in Raft after fencing the pointer
\* (or without pointer fencing in the UnfencedActivate negative-control variant).
FS018ActivateEpoch ==
    /\ p.raftPhase = "Fencing"
    /\ \/ /\ p.ptrEpoch = p.raftEpoch
          /\ p.ptrRev > 0
       \/ Variant = "UnfencedActivate"
    /\ p' = [p EXCEPT
        !.raftPhase = "Active",
        !.raftFencedRev = p.ptrRev,
        !.maxActivatedEpoch =
            IF p.raftEpoch > p.maxActivatedEpoch
            THEN p.raftEpoch
            ELSE p.maxActivatedEpoch]

\* Active sequencer stages an immutable candidate manifest at manifests/<gen>/<op>.json.
FS018StageCandidateManifest ==
    \E e \in Epochs, op \in Ops :
        /\ p.raftPhase = "Active"
        /\ p.raftEpoch = e
        /\ p.attemptOp[e] = 0
        /\ op \in p.prepared
        /\ op \notin p.ptrSplits
        /\ p.ptrGen < 2
        /\ LET candGen == p.ptrGen + 1
               candSplits == p.ptrSplits \cup {op}
               candManifest ==
                   [gen |-> candGen,
                    op |-> op,
                    parentGen |-> p.ptrGen,
                    parentOp |-> p.ptrOp,
                    epoch |-> e,
                    splits |-> candSplits]
           IN p' = [p EXCEPT
                !.manifests = @ \cup {candManifest},
                !.attemptOp[e] = op,
                !.attemptParentRev[e] = p.ptrRev,
                !.attemptParentGen[e] = p.ptrGen,
                !.attemptParentOp[e] = p.ptrOp,
                !.attemptParentSplits[e] = p.ptrSplits,
                !.attemptGen[e] = candGen]

\* Sequencer attempts to linearize its staged candidate manifest via pointer CAS.
FS018CommitPointer ==
    \E e \in Epochs :
        /\ p.attemptOp[e] # 0
        /\ p.ptrRev < 5
        /\ LET op == p.attemptOp[e]
               candGen == p.attemptGen[e]
               candSplits == p.attemptParentSplits[e] \cup {op}
               casMatches ==
                   /\ p.ptrRev = p.attemptParentRev[e]
                   /\ (p.ptrEpoch = e \/ Variant = "UnfencedActivate")
               canCommit ==
                   \/ casMatches
                   \/ Variant = "BlindOverwrite"
               staleCommit ==
                   /\ Variant # "BlindOverwrite"
                   /\ ((e < p.maxActivatedEpoch) \/ (e < p.maxFencedEpoch))
           IN
           /\ canCommit
           /\ p' = [p EXCEPT
                !.ptrRev = p.ptrRev + 1,
                !.ptrEpoch = IF e > p.ptrEpoch THEN e ELSE p.ptrEpoch,
                !.ptrGen = candGen,
                !.ptrOp = op,
                !.ptrSplits = candSplits,
                !.everCommittedSplits = @ \cup candSplits,
                !.staleEpochCommits =
                    IF staleCommit THEN @ \cup {<<op, e>>} ELSE @,
                !.attemptOp[e] = 0]

\* Stale or conflicted attempt observes pointer CAS conflict and marks candidate Superseded.
FS018AbortStaleAttempt ==
    \E e \in Epochs :
        /\ p.attemptOp[e] # 0
        /\ (p.ptrRev # p.attemptParentRev[e] \/ p.ptrEpoch > e)
        /\ p' = [p EXCEPT !.attemptOp[e] = 0]

\* Sequencer or retry repairs/writes publications/<op>/committed.json after pointer commit.
FS018RecordCommitted ==
    \E op \in Ops :
        /\ op \in p.ptrSplits
        /\ op \notin p.committed
        /\ p' = [p EXCEPT !.committed = @ \cup {op}]

FS018Next ==
    \/ FS018StageIntent
    \/ FS018PrepareBundle
    \/ FS018AcquireEpoch
    \/ FS018FencePointer
    \/ FS018ActivateEpoch
    \/ FS018StageCandidateManifest
    \/ FS018CommitPointer
    \/ FS018AbortStaleAttempt
    \/ FS018RecordCommitted

FS018TypeOK ==
    /\ p.raftPhase \in {"Unassigned", "Fencing", "Active"}
    /\ p.raftEpoch \in 0..2
    /\ p.raftFencedRev \in Revs
    /\ p.maxActivatedEpoch \in 0..2
    /\ p.maxFencedEpoch \in 0..2
    /\ p.ptrRev \in Revs
    /\ p.ptrEpoch \in 0..2
    /\ p.ptrGen \in 0..2
    /\ p.ptrOp \in 0..2
    /\ p.ptrSplits \subseteq Ops
    /\ p.everCommittedSplits \subseteq Ops
    /\ p.staleEpochCommits \subseteq (Ops \X Epochs)
    /\ p.intentFp \in [Ops -> 0..2]
    /\ p.acceptedFps \in [Ops -> SUBSET Fingerprints]
    /\ p.prepared \subseteq Ops
    /\ p.committed \subseteq Ops
    /\ p.manifests \subseteq ManifestRecordType
    /\ p.attemptOp \in [Epochs -> 0..2]
    /\ p.attemptParentRev \in [Epochs -> Revs]
    /\ p.attemptParentGen \in [Epochs -> 0..2]
    /\ p.attemptParentOp \in [Epochs -> 0..2]
    /\ p.attemptParentSplits \in [Epochs -> SUBSET Ops]
    /\ p.attemptGen \in [Epochs -> 0..2]

FS018PointerReferencesValidManifest ==
    p.ptrGen = 0 \/
        \E m \in p.manifests :
            /\ m.gen = p.ptrGen
            /\ m.op = p.ptrOp
            /\ m.splits = p.ptrSplits
            /\ m.epoch <= p.ptrEpoch

FS018MonotonicLineage ==
    \A m \in p.manifests :
        /\ m.parentGen = m.gen - 1
        /\ (m.parentGen = 0 => m.parentOp = 0)
        /\ (m.parentGen > 0 =>
            \E parent \in p.manifests :
                /\ parent.gen = m.parentGen
                /\ parent.op = m.parentOp
                /\ parent.splits \subseteq m.splits)

FS018NoLostCommittedSplit ==
    p.everCommittedSplits \subseteq p.ptrSplits

FS018NoStaleEpochCommit ==
    p.staleEpochCommits = {}

FS018UniqueOperationIdentity ==
    \A op \in Ops : Cardinality(p.acceptedFps[op]) <= 1

CommittedManifest(m) ==
    \/ (m.gen = p.ptrGen /\ m.op = p.ptrOp /\ m.splits = p.ptrSplits)
    \/ \E head \in p.manifests :
        /\ head.gen = p.ptrGen
        /\ head.op = p.ptrOp
        /\ head.splits = p.ptrSplits
        /\ head.parentGen = m.gen
        /\ head.parentOp = m.op

InFlightCandidateManifest(m) ==
    /\ p.attemptOp[m.epoch] = m.op
    /\ p.attemptGen[m.epoch] = m.gen

SupersededCandidateManifest(m) ==
    /\ ~CommittedManifest(m)
    /\ ~InFlightCandidateManifest(m)

FS018OrphanClassificationSound ==
    \A m \in p.manifests :
        \/ CommittedManifest(m)
        \/ InFlightCandidateManifest(m)
        \/ SupersededCandidateManifest(m)

=============================================================================
