# NYC Taxi Hybrid Benchmark Suite

This is the frozen benchmark loop for the current FerrisSearch grouped-analytics work.

## Safe Rebuild

Use the isolated January 2025 rebuild wrapper:

```bash
bash scripts/load_nyc_taxis_20m_bench.sh
```

That wrapper uses:

- a separate cluster data dir under `data/nyc_taxis_20m_bench_cluster`
- separate logs under `logs/nyc_taxis_20m_bench`
- separate ports (`19200` / `19300` base)
- the frozen benchmark runner `scripts/nyc_taxi_hybrid_benchmark.sh`

## Query Set

The benchmark suite intentionally mixes exact grouped scans and selective filtered analytics.

1. Carrier market share
2. Top pickup zones
3. Top routes (`PULocationID`, `DOLocationID`)
4. Airport corridor grouped analytics
5. WAV headline grouped analytics
6. Margin-gap grouped analytics

All queries run through `POST /nyc-taxis/_sql/explain` with `analyze=true` so the suite captures:

- wall clock time
- `execution_mode`
- `matched_hits`
- top-level SQL timings
- grouped coordinator breakdown under `timings.grouped_merge`

## Why This Suite

- `Top routes` is the current worst-case full-scan two-key grouped top-K query.
- `Top pickup zones` keeps the same shape with much lower bucket cardinality.
- `Airport corridor` and `WAV headline` measure filtered grouped behavior.
- `Margin gap` keeps arithmetic aggregate expressions in the loop.

This combination is the minimum suite we should rerun after each grouped collector or coordinator-merge optimization.
