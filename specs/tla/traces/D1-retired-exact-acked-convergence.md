# Retired D1 exact-acknowledged-state property

**Date:** September 28, 2026

**Configuration:** early `MC_D1_OrderFixed.cfg`

**TLC result:** unexpected violation of retired
`D1AcknowledgedCopiesConverge`

**Raw trace:** [`D1-retired-exact-acked-convergence.log`](D1-retired-exact-acked-convergence.log)

**Raw trace SHA-256:** `b4ee59f93ce96a78e3ff9549cca9647942aa3aa6fbcc3d03a79b4b7d9f495724`

TLC generated 1,996 states, found 1,404 distinct states before the violation,
and produced a depth-9 trace.

The primary had already applied sequence 1 while only sequence 0 had been
acknowledged. The replica held sequence 0. Requiring exact equality to the
latest acknowledged operation therefore rejected a safely ahead primary.

The property was replaced by:

- `NoCopyBehindAcked` during active concurrency; and
- logical state equality at quiescence when every primary-WAL operation was
  acknowledged.

This trace is a model-property correction, not evidence against D1.
