---------------------- MODULE MC_FS013_WriteAuthority ----------------------
\* Supplementary finite control-plane model, not the D1 data/recovery model.
\* Protected WAL verification and canonical resync are explicit abstractions.
\* Metadata commits linearize with quorum; observing their result is separate.

EXTENDS Naturals, FiniteSets

CONSTANTS Variant, MinimumCopies

VARIABLE s

Writes == 1..2
Epochs == 1..2
Variants == {"Safe", "NoJournal", "CachedRestart", "CrossEpochRevoke"}

EmptyAck ==
    [copies |-> 0, epoch |-> 0, term |-> 0, sealed |-> 0, fence |-> 0,
     phase |-> "PROMOTING", unrevoked |-> FALSE,
     healthy |-> FALSE, running |-> FALSE, verified |-> FALSE,
     debtClear |-> FALSE, journalClear |-> FALSE, captured |-> FALSE,
     current |-> FALSE, restarted |-> FALSE, restartFloor |-> 0]

AckType ==
    [copies : 0..2, epoch : 0..2, term : 0..2, sealed : 0..2, fence : 0..2,
     phase : {"ACTIVE", "PROMOTING"}, unrevoked : BOOLEAN,
     healthy : BOOLEAN, running : BOOLEAN, verified : BOOLEAN,
     debtClear : BOOLEAN, journalClear : BOOLEAN, captured : BOOLEAN,
     current : BOOLEAN, restarted : BOOLEAN, restartFloor : 0..2]

CurrentDebt == s.debt /\ s.intentEpoch = s.term
CopyCount == IF s.inSync THEN 2 ELSE 1

LiveAuthority ==
    /\ s.running /\ s.healthy /\ s.verified
    /\ s.phase = "ACTIVE"
    /\ s.epoch = s.term /\ s.sealed = s.term /\ s.fence = s.term
    /\ s.epoch \notin s.revoked

Admissible ==
    /\ LiveAuthority
    /\ ~CurrentDebt /\ ~s.journal
    /\ CopyCount >= MinimumCopies

ObservedExclusion ==
    s.observed /\ s.decision = "Accepted"

CapturedProof(id) ==
    \/ s.writeAllocation[id] = 0
    \/ s.replicaProof[id] = s.writeAllocation[id]
    \/ /\ s.exclusionSettled /\ ObservedExclusion
       /\ s.intentEpoch = s.writeEpoch[id]
       /\ s.intentAllocation = s.writeAllocation[id]

CurrentProof(id) ==
    ~s.inSync \/ s.replicaProof[id] = s.replicaAllocation

FS013Init ==
    /\ Variant \in Variants
    /\ MinimumCopies \in {1, 2}
    /\ s =
        [term |-> 1, phase |-> "ACTIVE", sealed |-> 1, fence |-> 1,
         epoch |-> 1, healthy |-> TRUE, verified |-> TRUE, running |-> TRUE,
         revoked |-> {}, nextWrite |-> 1, wal |-> {}, replicaWal |-> {},
         waiting |-> {}, unknown |-> {}, acked |-> {}, rejected |-> {},
         writeEpoch |-> [id \in Writes |-> 0],
         writeAllocation |-> [id \in Writes |-> 0],
         replicaProof |-> [id \in Writes |-> 0],
         ackProof |-> [id \in Writes |-> EmptyAck],
         inSync |-> TRUE, replicaAllocation |-> 1,
         committedInSync |-> TRUE, committedAllocation |-> 1,
         missingSeen |-> FALSE, intentEpoch |-> 0, intentAllocation |-> 0,
         debt |-> FALSE, journal |-> FALSE, decision |-> "None",
         proposalWasJournaled |-> FALSE, observed |-> FALSE,
         exclusionSettled |-> FALSE, settlements |-> {},
         rejectionEffects |-> {}, restartCount |-> 0, savedTerm |-> 0,
         journalAtRestart |-> FALSE, failureSeen |-> FALSE, failureWal |-> {},
         quorum |-> TRUE, quorumChanges |-> 0]

FS013BeginWrite ==
    LET id == s.nextWrite
    IN
    /\ id \in Writes
    /\ Admissible
    /\ s' = [s EXCEPT
        !.nextWrite = @ + 1,
        !.wal = @ \cup {id},
        !.waiting = @ \cup {id},
        !.writeEpoch[id] = s.epoch,
        !.writeAllocation[id] = s.replicaAllocation]

FS013RejectBeforeWal ==
    /\ s.nextWrite \in Writes
    /\ s.running /\ ~Admissible
    /\ s' = [s EXCEPT
        !.rejected = @ \cup {s.nextWrite},
        !.nextWrite = @ + 1]

FS013ReplicaProof ==
    \E id \in s.waiting :
        /\ LiveAuthority
        /\ s.writeEpoch[id] = s.epoch
        /\ s.inSync
        /\ s.writeAllocation[id] = s.replicaAllocation
        /\ s.replicaProof[id] = 0
        /\ s' = [s EXCEPT
            !.replicaWal = @ \cup {id},
            !.replicaProof[id] = s.replicaAllocation]

FS013MissingProof ==
    /\ ~s.missingSeen /\ 1 \in s.waiting
    /\ LiveAuthority /\ s.writeEpoch[1] = s.epoch
    /\ s.writeAllocation[1] > 0 /\ s.replicaProof[1] = 0
    /\ s' = [s EXCEPT
        !.missingSeen = TRUE,
        !.intentEpoch = s.writeEpoch[1],
        !.intentAllocation = s.writeAllocation[1],
        !.debt = TRUE]

FS013Deadline ==
    /\ s.missingSeen /\ 1 \in s.waiting
    /\ s' = [s EXCEPT
        !.waiting = @ \ {1},
        !.unknown = @ \cup {1}]

FS013Journal ==
    /\ LiveAuthority /\ s.intentEpoch = s.epoch
    /\ s.debt /\ ~s.journal
    /\ s' = [s EXCEPT !.journal = TRUE]

FS013Propose ==
    /\ LiveAuthority /\ s.intentEpoch = s.epoch
    /\ s.debt /\ s.decision = "None"
    /\ (s.journal \/ Variant = "NoJournal")
    /\ s' = [s EXCEPT
        !.decision = "Queued",
        !.proposalWasJournaled = s.journal]

FS013Commit ==
    LET accepted ==
        /\ s.term = s.intentEpoch /\ s.phase = "ACTIVE"
        /\ s.committedInSync
        /\ s.committedAllocation = s.intentAllocation
    IN
    /\ s.decision = "Queued" /\ s.quorum
    /\ s' = [s EXCEPT
        !.decision = IF accepted THEN "Accepted" ELSE "Rejected",
        !.committedInSync = IF accepted THEN FALSE ELSE @,
        !.committedAllocation = IF accepted THEN 0 ELSE @]

FS013ObserveCommit ==
    LET revoke ==
        IF Variant = "CrossEpochRevoke" /\ s.epoch > 0
        THEN s.epoch ELSE s.intentEpoch
    IN
    /\ s.running /\ ~s.observed
    /\ s.decision \in {"Accepted", "Rejected"}
    /\ IF s.decision = "Accepted"
       THEN s' = [s EXCEPT
            !.observed = TRUE,
            !.inSync = IF s.replicaAllocation = s.intentAllocation
                       THEN FALSE ELSE @,
            !.replicaAllocation = IF @ = s.intentAllocation THEN 0 ELSE @]
       ELSE s' = [s EXCEPT
            !.observed = TRUE,
            !.revoked = @ \cup {revoke},
            !.epoch = IF @ = revoke THEN 0 ELSE @,
            !.rejectionEffects =
                @ \cup {[rejectedEpoch |-> s.intentEpoch,
                         revokedEpoch |-> revoke]}]

FS013Settle ==
    LET newer == s.term > s.intentEpoch
        reassigned ==
            s.inSync /\ s.replicaAllocation \notin {0, s.intentAllocation}
    IN
    /\ s.running /\ (s.debt \/ s.journal)
    /\ ObservedExclusion \/ newer \/ reassigned
    /\ s' = [s EXCEPT
        !.debt = FALSE,
        !.journal = FALSE,
        !.exclusionSettled = @ \/ ObservedExclusion,
        !.settlements =
            @ \cup {[observedAccepted |-> ObservedExclusion,
                     newerTerm |-> newer, reassigned |-> reassigned]}]

FS013Reserve ==
    /\ s.running /\ s.healthy /\ s.verified /\ s.quorum
    /\ s.term < 2
    /\ s' = [s EXCEPT
        !.term = @ + 1,
        !.phase = "PROMOTING",
        !.epoch = 0,
        !.revoked = @ \cup (IF s.epoch = 0 THEN {} ELSE {s.epoch}),
        !.unknown = @ \cup s.waiting,
        !.waiting = {},
        !.inSync = s.committedInSync,
        !.replicaAllocation = s.committedAllocation]

\* Successful canonical resync preserves all protected primary operations.
\* Peer-window comparison, bytes, gaps, and unique peer adoption are not modeled.
FS013Seal ==
    /\ s.running /\ s.healthy /\ s.verified
    /\ s.phase = "PROMOTING" /\ s.sealed < s.term
    /\ s' = [s EXCEPT
        !.sealed = s.term,
        !.fence = s.term,
        !.replicaWal = IF s.inSync THEN s.wal ELSE @]

FS013Finish ==
    /\ s.running /\ s.healthy /\ s.verified /\ s.quorum
    /\ s.phase = "PROMOTING" /\ s.sealed = s.term /\ s.fence = s.term
    /\ s' = [s EXCEPT !.phase = "ACTIVE"]

FS013InstallEpoch ==
    /\ s.running /\ s.healthy /\ s.verified
    /\ s.phase = "ACTIVE" /\ s.epoch = 0
    /\ s.sealed = s.term /\ s.fence = s.term
    /\ s.term \notin s.revoked
    /\ ~CurrentDebt /\ ~s.journal
    /\ s.restartCount = 0 \/ s.term > s.savedTerm
    /\ s' = [s EXCEPT !.epoch = s.term]

\* Collapse removal, fresh assignment, and verified recovery into one action.
FS013ReplaceReplica ==
    /\ LiveAuthority /\ s.quorum
    /\ s.intentEpoch > 0 /\ s.term > s.intentEpoch
    /\ ~CurrentDebt /\ ~s.journal
    /\ s.replicaAllocation # 2
    /\ s' = [s EXCEPT
        !.inSync = TRUE, !.replicaAllocation = 2,
        !.committedInSync = TRUE, !.committedAllocation = 2,
        !.replicaWal = s.wal]

FS013FailAfterWal ==
    /\ s.running /\ s.healthy /\ ~s.failureSeen /\ s.wal # {}
    /\ s' = [s EXCEPT
        !.healthy = FALSE,
        !.failureSeen = TRUE,
        !.failureWal = s.wal,
        !.revoked = @ \cup (IF s.epoch = 0 THEN {} ELSE {s.epoch}),
        !.epoch = 0]

FS013Crash ==
    /\ s.running /\ s.restartCount = 0
    /\ s' = [s EXCEPT
        !.running = FALSE, !.verified = FALSE, !.epoch = 0,
        !.waiting = {},
        !.unknown = @ \cup s.waiting,
        !.debt = s.journal]

FS013Restart ==
    /\ ~s.running /\ s.restartCount = 0
    /\ s' = [s EXCEPT
        !.running = TRUE, !.healthy = TRUE, !.verified = FALSE,
        !.restartCount = 1, !.savedTerm = s.term,
        !.journalAtRestart = s.journal, !.revoked = {}]

FS013VerifyProtected ==
    /\ s.running /\ s.healthy /\ ~s.verified
    /\ s' = [s EXCEPT !.verified = TRUE]

FS013CachedRestart ==
    /\ Variant = "CachedRestart"
    /\ s.restartCount = 1 /\ s.term = s.savedTerm
    /\ s.running /\ s.healthy /\ s.verified /\ s.epoch = 0
    /\ s.phase = "ACTIVE" /\ s.sealed = s.term /\ s.fence = s.term
    /\ ~CurrentDebt /\ ~s.journal
    /\ s' = [s EXCEPT !.epoch = s.term]

FS013Ack ==
    \E id \in s.waiting :
        /\ Admissible /\ s.writeEpoch[id] = s.epoch
        /\ id \in s.wal /\ CapturedProof(id) /\ CurrentProof(id)
        /\ s' = [s EXCEPT
            !.waiting = @ \ {id},
            !.acked = @ \cup {id},
            !.ackProof[id] =
                [copies |-> CopyCount, epoch |-> s.epoch, term |-> s.term,
                 sealed |-> s.sealed, fence |-> s.fence,
                 phase |-> s.phase, unrevoked |-> s.epoch \notin s.revoked,
                 healthy |-> s.healthy, running |-> s.running,
                 verified |-> s.verified, debtClear |-> ~CurrentDebt,
                 journalClear |-> ~s.journal, captured |-> CapturedProof(id),
                 current |-> CurrentProof(id),
                 restarted |-> s.restartCount = 1,
                 restartFloor |-> s.savedTerm]]

FS013Quorum ==
    /\ s.quorumChanges < 2
    /\ s' = [s EXCEPT !.quorum = ~@, !.quorumChanges = @ + 1]

FS013Next ==
    \/ FS013BeginWrite \/ FS013RejectBeforeWal \/ FS013ReplicaProof
    \/ FS013MissingProof \/ FS013Deadline \/ FS013Journal
    \/ FS013Propose \/ FS013Commit \/ FS013ObserveCommit \/ FS013Settle
    \/ FS013Reserve \/ FS013Seal \/ FS013Finish \/ FS013InstallEpoch
    \/ FS013ReplaceReplica \/ FS013FailAfterWal
    \/ FS013Crash \/ FS013Restart \/ FS013VerifyProtected
    \/ FS013CachedRestart \/ FS013Ack \/ FS013Quorum

FS013TypeOK ==
    /\ s.term \in Epochs /\ s.phase \in {"ACTIVE", "PROMOTING"}
    /\ s.sealed \in Epochs /\ s.fence \in Epochs /\ s.epoch \in 0..2
    /\ s.healthy \in BOOLEAN /\ s.verified \in BOOLEAN /\ s.running \in BOOLEAN
    /\ s.revoked \subseteq Epochs /\ s.nextWrite \in 1..3
    /\ s.wal \subseteq Writes /\ s.replicaWal \subseteq Writes
    /\ s.waiting \subseteq Writes /\ s.unknown \subseteq Writes
    /\ s.acked \subseteq Writes /\ s.rejected \subseteq Writes
    /\ s.writeEpoch \in [Writes -> 0..2]
    /\ s.writeAllocation \in [Writes -> 0..2]
    /\ s.replicaProof \in [Writes -> 0..2]
    /\ s.ackProof \in [Writes -> AckType]
    /\ s.inSync \in BOOLEAN /\ s.replicaAllocation \in 0..2
    /\ s.committedInSync \in BOOLEAN /\ s.committedAllocation \in 0..2
    /\ s.inSync = (s.replicaAllocation # 0)
    /\ s.committedInSync = (s.committedAllocation # 0)
    /\ s.missingSeen \in BOOLEAN /\ s.intentEpoch \in 0..2
    /\ s.intentAllocation \in 0..2 /\ s.debt \in BOOLEAN /\ s.journal \in BOOLEAN
    /\ s.decision \in {"None", "Queued", "Accepted", "Rejected"}
    /\ s.proposalWasJournaled \in BOOLEAN /\ s.observed \in BOOLEAN
    /\ s.exclusionSettled \in BOOLEAN
    /\ s.settlements \subseteq
        [observedAccepted : BOOLEAN, newerTerm : BOOLEAN, reassigned : BOOLEAN]
    /\ s.rejectionEffects \subseteq
        [rejectedEpoch : Epochs, revokedEpoch : Epochs]
    /\ s.restartCount \in 0..1 /\ s.savedTerm \in 0..2
    /\ s.journalAtRestart \in BOOLEAN /\ s.failureSeen \in BOOLEAN
    /\ s.failureWal \subseteq Writes
    /\ s.quorum \in BOOLEAN /\ s.quorumChanges \in 0..2

FS013JournalBeforeProposal ==
    s.decision = "None" \/ s.proposalWasJournaled

FS013ObservedSettlement ==
    \A evidence \in s.settlements :
        evidence.observedAccepted \/ evidence.newerTerm \/ evidence.reassigned

FS013NoAckWithDebt ==
    \A id \in s.acked :
        s.ackProof[id].debtClear /\ s.ackProof[id].journalClear

FS013MinimumCopies ==
    \A id \in s.acked : s.ackProof[id].copies >= MinimumCopies

FS013AckAuthority ==
    \A id \in s.acked :
        LET proof == s.ackProof[id]
        IN
        /\ proof.healthy /\ proof.running /\ proof.verified
        /\ proof.phase = "ACTIVE" /\ proof.unrevoked
        /\ proof.epoch = proof.term /\ proof.sealed = proof.term
        /\ proof.fence = proof.term /\ proof.epoch \in Epochs
        /\ proof.captured /\ proof.current

FS013RetainedHistory ==
    /\ s.acked \subseteq s.wal
    /\ s.inSync => s.acked \subseteq s.replicaWal

FS013RejectedBeforeWal == s.rejected \cap s.wal = {}
FS013UnknownNeverAck == s.unknown \cap s.acked = {}

FS013FailStop ==
    s.running /\ ~s.healthy => s.wal = s.failureWal /\ s.epoch = 0

FS013FreshRestartAuthority ==
    \A id \in s.acked :
        s.ackProof[id].restarted =>
            s.ackProof[id].epoch > s.ackProof[id].restartFloor

FS013OldRejectionScoped ==
    \A effect \in s.rejectionEffects :
        effect.rejectedEpoch = effect.revokedEpoch

FS013NoDurableRestartWitness ==
    ~(/\ s.journalAtRestart
      /\ 1 \in s.unknown /\ 2 \in s.acked
      /\ s.ackProof[2].epoch > s.savedTerm
      /\ 1 \in s.wal)

=============================================================================
