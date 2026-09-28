--------------------- MODULE MC_D1_PromotionReplayNoOp ----------------------
\* A promoted copy replays every local WAL entry before filling an
\* unacknowledged gap with a NoOp. Replication of that NoOp may fail, leaving a
\* replica-local gap, but activation does not wait for the failed replication.

EXTENDS Naturals, FiniteSets, TLC

VARIABLES
    phase,
    walEntries,
    replayed,
    primaryProcessed,
    primaryCheckpoint,
    noOps,
    replicaProcessed,
    replicaCheckpoint,
    noOpReplicationFailed,
    activated,
    acknowledged

vars ==
    <<phase, walEntries, replayed, primaryProcessed, primaryCheckpoint, noOps,
      replicaProcessed, replicaCheckpoint, noOpReplicationFailed, activated,
      acknowledged>>

ContiguousNext(values) ==
    CHOOSE boundary \in 0..3 :
        /\ {seq \in 0..2 : seq < boundary} \subseteq values
        /\ (boundary = 3 \/ boundary \notin values)

B4Init ==
    /\ phase = 0
    /\ walEntries = {0, 2}
    /\ replayed = {}
    /\ primaryProcessed = {}
    /\ primaryCheckpoint = 0
    /\ noOps = {}
    /\ replicaProcessed = {0, 2}
    /\ replicaCheckpoint = 1
    /\ noOpReplicationFailed = FALSE
    /\ activated = FALSE
    /\ acknowledged = {0, 2}

B4TypeOK ==
    /\ phase \in 0..4
    /\ walEntries \subseteq 0..2
    /\ replayed \subseteq 0..2
    /\ primaryProcessed \subseteq 0..2
    /\ primaryCheckpoint \in 0..3
    /\ noOps \subseteq 0..2
    /\ replicaProcessed \subseteq 0..2
    /\ replicaCheckpoint \in 0..3
    /\ noOpReplicationFailed \in BOOLEAN
    /\ activated \in BOOLEAN
    /\ acknowledged \subseteq 0..2

B4ReplayWalEntry(seq) ==
    /\ phase = 0
    /\ seq \in walEntries \ replayed
    /\ replayed' = replayed \cup {seq}
    /\ primaryProcessed' = primaryProcessed \cup {seq}
    /\ primaryCheckpoint' =
          ContiguousNext(primaryProcessed \cup {seq})
    /\ IF replayed \cup {seq} = walEntries
          THEN phase' = 1
          ELSE phase' = phase
    /\ UNCHANGED
          <<walEntries, noOps, replicaProcessed, replicaCheckpoint,
            noOpReplicationFailed, activated, acknowledged>>

B4FillNoOpAfterReplay ==
    /\ phase = 1
    /\ replayed = walEntries
    /\ noOps' = {1}
    /\ primaryProcessed' = 0..2
    /\ primaryCheckpoint' = 3
    /\ phase' = 2
    /\ UNCHANGED
          <<walEntries, replayed, replicaProcessed, replicaCheckpoint,
            noOpReplicationFailed, activated, acknowledged>>

B4NoOpReplicationFails ==
    /\ phase = 2
    /\ noOpReplicationFailed' = TRUE
    /\ phase' = 3
    /\ UNCHANGED
          <<walEntries, replayed, primaryProcessed, primaryCheckpoint, noOps,
            replicaProcessed, replicaCheckpoint, activated, acknowledged>>

B4ActivatePrimary ==
    /\ phase = 3
    /\ primaryCheckpoint = 3
    /\ activated' = TRUE
    /\ phase' = 4
    /\ UNCHANGED
          <<walEntries, replayed, primaryProcessed, primaryCheckpoint, noOps,
            replicaProcessed, replicaCheckpoint, noOpReplicationFailed,
            acknowledged>>

B4Next ==
    \/ \E seq \in 0..2 : B4ReplayWalEntry(seq)
    \/ B4FillNoOpAfterReplay
    \/ B4NoOpReplicationFails
    \/ B4ActivatePrimary

B4NoOpOnlyAfterReplay ==
    noOps # {} => replayed = walEntries

B4CheckpointGapAware ==
    /\ primaryCheckpoint = ContiguousNext(primaryProcessed)
    /\ replicaCheckpoint = ContiguousNext(replicaProcessed)

B4NoCopyBehindAcked ==
    /\ acknowledged \subseteq replicaProcessed
    /\ activated => acknowledged \subseteq primaryProcessed

B4ActivationAfterReplayAndFill ==
    activated =>
        /\ replayed = walEntries
        /\ 1 \in noOps
        /\ primaryCheckpoint = 3

B4FailedNoOpReplicationDoesNotBlockActivation ==
    phase = 4 =>
        /\ noOpReplicationFailed
        /\ activated
        /\ 1 \notin replicaProcessed
        /\ replicaCheckpoint = 1

=============================================================================
