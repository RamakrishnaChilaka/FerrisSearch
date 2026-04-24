# Remote Store RustFS Live Runbook

This runbook exercises the real `remote_store` path against:

- the RustFS S3-compatible dev server
- a real 3-node FerrisSearch release cluster started with `./dev_cluster_release.sh --nodes 3`
- cross-node coordinator forwarding for create, publish, and search
- remote manifest and split object creation in the shared object store
- node-local warm split-cache hydration after search

It complements the automated integration tests. It is not a replacement for them.

For an isolated scripted smoke run of the automatable pieces, use `./scripts/remote_store_rustfs_smoke.sh`. That script starts RustFS if needed, launches three isolated release nodes on ports `19200-19202` / `19300-19302`, validates create/publish/search/query-string/count/verify/object-layout/cache-layout, and leaves the RustFS console inspection as the remaining manual step.

## What This Validates

The known-good live path validated by this runbook is:

1. start RustFS
2. start a 3-node FerrisSearch release cluster pointed at an `s3://...` prefix
3. create a `remote_store` index through a non-leader HTTP node
4. publish one split through a different HTTP node
5. search that split through `POST /{index}/_search` with a DSL body
6. verify the split checksum through `POST /{index}/_remote_store/verify`
7. inspect the remote prefix in the RustFS console
8. inspect the hydrated node-local split cache on a data node

## Current Automated Coverage

The automated tests already cover part of this surface:

- `tests/rest_api_integration.rs::remote_store_search_returns_hits_from_published_split`
- `tests/rest_api_integration.rs::remote_store_query_string_search_returns_hits_from_published_split`
- `tests/rest_api_integration.rs::remote_store_count_match_all_uses_manifest_doc_counts`
- `tests/rest_api_integration.rs::remote_store_search_fans_out_from_master_only_coordinator`
- `tests/rest_api_integration.rs::remote_store_publish_appends_across_generations`
- `tests/rest_api_integration.rs::remote_store_verify_reports_ok_for_untouched_splits`
- `tests/rest_api_integration.rs::remote_store_verify_reports_mismatch_after_file_tamper`
- `tests/remote_store_s3_integration.rs` covers `StorageManager` manifest/object operations against a real S3-compatible endpoint when `FERRIS_RUSTFS_ENDPOINT` is set

What those tests do **not** currently replace:

- process-backed validation with `./dev_cluster_release.sh --nodes 3`
- RustFS console inspection
- manual confirmation of the concrete remote prefix layout after a publish
- manual confirmation of the warmed node-local cache layout after a live search

## Prerequisites

- Docker available locally
- RustFS dev server script: `./scripts/rustfs_dev.sh`
- release cluster launcher: `./dev_cluster_release.sh --nodes 3`

## 1. Start RustFS

```bash
./scripts/rustfs_dev.sh up
eval "$(./scripts/rustfs_dev.sh env)"
```

Expected values:

- bucket: `ferrissearch-dev`
- S3 API: `http://127.0.0.1:9100`
- console: `http://127.0.0.1:9101`
- credentials: `rustfsadmin` / `rustfsadmin`

## 2. Start The Release Cluster

Choose a unique remote prefix and launch the real release cluster:

```bash
export LIVE_REMOTE_PREFIX="live-test-$(date +%s)"
export FERRISSEARCH_STORAGE_URI="s3://$FERRIS_REMOTE_STORE_BUCKET/$LIVE_REMOTE_PREFIX"

./dev_cluster_release.sh --nodes 3
```

Wait for:

- all three nodes to bind `9200`, `9201`, `9202`
- node 1 to bootstrap as leader
- nodes 2 and 3 to join the cluster

Sanity check:

```bash
curl -sS http://127.0.0.1:9200/_cluster/health
```

Expected shape:

```json
{
  "cluster_name": "ferrissearch-local",
  "status": "green",
  "number_of_nodes": 3,
  "number_of_data_nodes": 3,
  "unassigned_shards": 0
}
```

## 3. Create A Remote Store Index Through A Follower

Use a non-leader node on purpose so the coordinator-forwarding path is exercised:

```bash
curl -sS -X PUT http://127.0.0.1:9201/warmremote \
  -H 'Content-Type: application/json' \
  -d '{"engine":"remote_store"}'
```

Expected result: HTTP `200` and the index appears in cluster state.

## 4. Publish One Split Through Another Node

```bash
curl -sS -X POST http://127.0.0.1:9202/warmremote/_remote_store/publish \
  -H 'Content-Type: application/json' \
  -d '{
    "docs": [
      {"_id":"doc-1","title":"warm remote store hit","body":"first published split for rustfs"},
      {"_id":"doc-2","title":"second warm remote store hit","body":"second document in the same published split"}
    ]
  }'
```

Expected shape:

```json
{
  "generation": 1,
  "doc_count": 2,
  "split_id": "..."
}
```

## 5. Run Known-Good Searches

Use `POST /{index}/_search` with a DSL body for live validation.

Match all:

```bash
curl -sS -X POST http://127.0.0.1:9201/warmremote/_search \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match_all":{}},"size":10}'
```

Expected result:

- `_shards.successful = 1`
- `_shards.failed = 0`
- `hits.total.value = 2`
- both docs returned from the same `_split`

Text match on the analyzed `title` field:

```bash
curl -sS -X POST http://127.0.0.1:9201/warmremote/_search \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match":{"title":"warm"}},"size":10}'
```

Text match on the analyzed `body` field:

```bash
curl -sS -X POST http://127.0.0.1:9202/warmremote/_search \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match":{"body":"published"}},"size":10}'
```

Query-string search over the catch-all `body` field:

```bash
curl -sS 'http://127.0.0.1:9200/warmremote/_search?q=warm'
```

Match-all count using manifest doc-count metadata:

```bash
curl -sS -X POST http://127.0.0.1:9202/warmremote/_count \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match_all":{}}}'
```

Notes:

- `title` is a `text` field here, so `term` queries against the full title string are not a good validation query. Use `match` instead.
- This runbook intentionally uses the DSL `POST /_search` path because that is the path already covered by the current `remote_store` REST integration tests.
- `GET /{index}/_search?q=...` and match-all `POST /{index}/_count` now have dedicated REST regressions, so they are part of the supported remote_store read surface for this flow.

## 6. Verify The Published Split

```bash
curl -sS -X POST http://127.0.0.1:9200/warmremote/_remote_store/verify \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Expected shape:

```json
{
  "generation": 1,
  "ok_count": 1,
  "mismatch_count": 0,
  "missing_count": 0,
  "unsupported_count": 0,
  "splits": [
    {"split_id":"...","status":"ok"}
  ]
}
```

## 7. Inspect The RustFS Console

Open:

- `http://127.0.0.1:9101/rustfs/console/browser/?bucket=ferrissearch-dev`

Log in with:

- username: `rustfsadmin`
- password: `rustfsadmin`

Walk the object tree:

1. bucket root: `live-test-<timestamp>/`
2. inside the prefix: one index UUID directory
3. inside the UUID directory:
   - `manifest.current.json`
   - `manifests/`
   - `splits/`

Expected object layout:

```text
live-test-<timestamp>/<index_uuid>/
  manifest.current.json
  manifests/000000000001.json
  splits/<split_id>/bundle
```

Useful preview checks:

- `manifest.current.json` should point at `manifests/000000000001.json`
- `manifests/000000000001.json` should include:
  - `engine: "remote_store"`
  - `index_name: "warmremote"`
  - `generation: 1`
  - one published split
  - `bundle_path` ending in `splits/<split_id>/bundle`
  - `doc_count: 2`

## 8. Inspect The Warm Local Cache

After a successful search, at least one node should hydrate the split under its local cache.

Example path:

```text
data/node-1/_remote_store_cache/splits/<index_uuid>/<split_id>/
```

Expected artifacts include:

```text
.done
index/meta.json
translog.manifest
translog.seqno
```

One concrete check:

```bash
find data/node-1/_remote_store_cache data/node-2/_remote_store_cache data/node-3/_remote_store_cache \
  -maxdepth 6 -type f 2>/dev/null | sort
```

At least one node should show a hydrated split directory after the search step.

## 9. Stop Everything

- stop the release cluster terminal with `Ctrl+C`
- optionally stop RustFS:

```bash
./scripts/rustfs_dev.sh down
```

## Live Validation Snapshot

The live validation on 2026-04-24 produced this concrete shape:

- prefix: `live-test-1777004862`
- index UUID: `08063c32-7232-4ef1-9041-87607cdfb10b`
- split ID: `ed6fc38a-2464-460b-ba17-04d86dd26e48`
- manifest generation: `1`
- docs returned by DSL search: `2`
- verify result: `ok_count = 1`, `mismatch_count = 0`

This is the reference flow to re-run when a `remote_store` change needs a real object-store sanity check outside the automated suite.
