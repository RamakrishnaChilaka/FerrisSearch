------------------------- MODULE MC_D1_PrimaryGap ---------------------------
\* The primary WAL contains sequence 1, but engine apply failed. Both primary
\* and replica have processed {0,2}, checkpoint 1, and max sequence 2.
\* Comparing the replica checkpoint to primary max loops recovery; comparing
\* processed checkpoints recognizes that the replica is equally complete.

EXTENDS Naturals, FiniteSets, TLC

CONSTANT DetectorMode

VARIABLES
    phase,
    primaryProcessed,
    primaryCheckpoint,
    primaryMaxNext,
    replicaProcessed,
    replicaCheckpoint,
    recoveryCount,
    acknowledged

vars ==
    <<phase, primaryProcessed, primaryCheckpoint, primaryMaxNext,
      replicaProcessed, replicaCheckpoint, recoveryCount, acknowledged>>

B3Init ==
    /\ phase = 0
    /\ primaryProcessed = {}
    /\ primaryCheckpoint = 0
    /\ primaryMaxNext = 0
    /\ replicaProcessed = {}
    /\ replicaCheckpoint = 0
    /\ recoveryCount = 0
    /\ acknowledged = {}

B3TypeOK ==
    /\ phase \in 0..3
    /\ primaryProcessed \subseteq 0..2
    /\ primaryCheckpoint \in 0..3
    /\ primaryMaxNext \in 0..3
    /\ replicaProcessed \subseteq 0..2
    /\ replicaCheckpoint \in 0..3
    /\ recoveryCount \in 0..2
    /\ acknowledged \subseteq 0..2

B3CreatePrimaryAndReplicaGap ==
    /\ phase = 0
    /\ primaryProcessed' = {0, 2}
    /\ primaryCheckpoint' = 1
    /\ primaryMaxNext' = 3
    /\ replicaProcessed' = {0, 2}
    /\ replicaCheckpoint' = 1
    /\ acknowledged' = {0, 2}
    /\ phase' = 1
    /\ UNCHANGED recoveryCount

\* Historical detector compares replica checkpoint 1 with primary max next 3,
\* re-recovers the identical gap, and repeats.
B3MaxBasedRecover ==
    /\ DetectorMode = "MaxBased"
    /\ phase \in {1, 2}
    /\ replicaCheckpoint < primaryMaxNext
    /\ recoveryCount' = recoveryCount + 1
    /\ replicaProcessed' = primaryProcessed
    /\ replicaCheckpoint' = primaryCheckpoint
    /\ phase' = phase + 1
    /\ UNCHANGED
          <<primaryProcessed, primaryCheckpoint, primaryMaxNext,
            acknowledged>>

\* Fixed detector compares processed checkpoints. Equal gaps need no recovery.
B3ProcessedBasedStable ==
    /\ DetectorMode = "ProcessedBased"
    /\ phase = 1
    /\ replicaCheckpoint = primaryCheckpoint
    /\ phase' = 3
    /\ UNCHANGED
          <<primaryProcessed, primaryCheckpoint, primaryMaxNext,
            replicaProcessed, replicaCheckpoint, recoveryCount, acknowledged>>

B3Next ==
    \/ B3CreatePrimaryAndReplicaGap
    \/ B3MaxBasedRecover
    \/ B3ProcessedBasedStable

B3NoCopyBehindAcked ==
    /\ acknowledged \subseteq primaryProcessed
    /\ acknowledged \subseteq replicaProcessed

B3NoRecoveryLoop == recoveryCount <= 1

B3ProcessedComparisonAvoidsRecovery ==
    DetectorMode = "ProcessedBased" => recoveryCount = 0

=============================================================================
