------------------ MODULE MC_L2_PrimaryRestart_IdleShard -------------------
\* Reviewer-named idle-shard check. No client request follows the primary
\* restart. Progress therefore depends on the node lifecycle's weakly fair
\* proactive activation action inherited from MC_L2_PrimaryRestart.

EXTENDS MC_L2_PrimaryRestart

IdleShardPrimaryRestartSpec == PrimaryRestartSpec

=============================================================================
