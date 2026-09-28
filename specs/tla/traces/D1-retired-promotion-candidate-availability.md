# Retired D1 promotion-candidate availability property

**Date:** September 28, 2026

**Configuration:** early `MC_D1_PromotionReplayNoOp.cfg`

**TLC result:** unexpected initial-state violation of an availability-mis-scoped
`B4NoCopyBehindAcked`

**Raw trace:** [`D1-retired-promotion-candidate-availability.log`](D1-retired-promotion-candidate-availability.log)

**Raw trace SHA-256:** `2bc999af28ab8e2f38ef1a7e07283f9a93bfb1a6180a48a3a2fee62ebc9af1db`

The first property required the promotion candidate to contain every
acknowledged operation before local WAL replay began. The candidate was not
activated and was not yet an available primary; the existing in-sync replica
already contained all acknowledged operations.

The corrected property always checks available in-sync replicas, but checks
the promotion candidate only after WAL replay, NoOp gap fill, and activation.
This is an availability-scope correction, not evidence against D1.

