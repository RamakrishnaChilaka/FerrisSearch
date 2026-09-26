---------------------------- MODULE PeerRecovery ----------------------------
\* The first incremental check builds RaftLog + ShardReplication.  Recovery
\* state and actions are added in the next modeling step without changing the
\* write/metadata semantics already checked by C1 and C2.

EXTENDS ShardReplication

PeerRecoveryVars == <<>>

PeerRecoveryInit == TRUE

PeerRecoveryTypeOK == TRUE

PeerRecoveryNext == FALSE

=============================================================================
