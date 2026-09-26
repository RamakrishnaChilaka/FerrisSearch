----------------------------- MODULE Invariants -----------------------------
EXTENDS Faults

vars == <<RaftVars, ReplicationVars, PeerRecoveryVars, FaultVars>>

Init ==
    /\ ReplicationInit
    /\ PeerRecoveryInit
    /\ FaultInit

Next ==
    \/ /\ ReplicationNext
       /\ UNCHANGED <<PeerRecoveryVars, FaultVars>>
    \/ /\ PeerRecoveryNext
       /\ UNCHANGED FaultVars
    \/ FaultNext

TypeOK ==
    /\ RaftTypeOK
    /\ ReplicationTypeOK
    /\ PeerRecoveryTypeOK
    /\ FaultTypeOK

RoutingWellFormed ==
    /\ RoutingWellFormedValue(routing)
    /\ termMonotonic
    /\ \A entry \in {raftLog[i] : i \in 1..Len(raftLog)} :
           RoutingWellFormedValue(entry.state)

NoAckedLoss ==
    \A writeId \in acked :
        /\ IF copyExists[routing.primary]
              THEN writeId \in ops[routing.primary]
              ELSE TRUE
        /\ \A replica \in routing.inSync :
               IF copyExists[replica]
               THEN writeId \in ops[replica]
               ELSE TRUE

PromotionComplete == promotionSafe

AdmissionComplete ==
    /\ admissionSafe
    /\ ackMembershipSafe

UniqueAckedSeq ==
    \A first \in acked :
      \A second \in acked :
        writeSeq[first] = writeSeq[second] => first = second

LatestAckedSeq(doc) ==
    LET acknowledgedForDoc == {w \in acked : writeDoc[w] = doc}
        seqs == {writeSeq[w] : w \in acknowledgedForDoc}
    IN IF seqs = {}
       THEN 0
       ELSE CHOOSE value \in seqs : \A other \in seqs : other <= value

NoAckedRollback ==
    \A doc \in Docs :
      LET acknowledgedForDoc == {w \in acked : writeDoc[w] = doc}
          currentWrite == docValue[routing.primary][doc]
      IN IF acknowledgedForDoc = {} \/ ~copyExists[routing.primary]
         THEN TRUE
         ELSE /\ currentWrite # NoWrite
              /\ writeSeq[currentWrite] >= LatestAckedSeq(doc)

\* Set by PeerRecovery.InstallSnapshot before any destructive install.
NoAuthoritativeWipe == authoritativeWipeSafe

NoPartialServe ==
    \A node \in Nodes :
        installMarker[node] =>
            /\ node # routing.primary
            /\ node \notin routing.inSync
            /\ BlocksLiveReplication(node)

NoApplyBelowObservedFence == staleApplySafe

ActivePrimaryRejectsOldTerm == activePrimaryApplySafe

BarrierReleased ==
    \A node \in Nodes :
        (exclusiveHolder[node] # NoNode)
        ~> (exclusiveHolder[node] = NoNode)

RecoveryConverges ==
    \A node \in Nodes :
        (node \in routing.replicas)
        ~> (node \in routing.inSync \/ node = routing.primary)

PendingResolves ==
    \A node \in Nodes :
        (copyMode[node] = "Pending")
        ~> (copyMode[node] # "Pending")

SafetyConstraint ==
    /\ Len(raftLog) <= MaxRaftEntries
    /\ Cardinality(pendingRaft) <= MaxPendingRaft
    /\ Cardinality(messages) <= MaxMessages
    /\ Cardinality(ActiveWrites) <= 1
    /\ \A node \in Nodes :
           \/ ~raftConnected[node]
           \/ Len(raftLog) - applied[node] <= MaxViewLag

SafetySpec == Init /\ [][Next]_vars

=============================================================================
