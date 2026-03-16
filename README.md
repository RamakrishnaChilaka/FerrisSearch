# ROpenSearch

<p align="center">
  <strong>A distributed search engine written in Rust, powered by <a href="https://github.com/quickwit-oss/tantivy">Tantivy</a></strong>
</p>

<p align="center">
  <a href="#getting-started">Getting Started</a> &middot;
  <a href="#api-reference">API Reference</a> &middot;
  <a href="#replication">Replication</a> &middot;
  <a href="#testing">Testing</a>
</p>

---

ROpenSearch is a lightweight, Rust-native search engine with OpenSearch-compatible REST APIs. Built for teams that want the familiar OpenSearch interface with the performance and safety of Rust.

## Highlights

- **OpenSearch-compatible REST API** — drop-in `PUT /{index}`, `POST /_doc`, `GET /_search` endpoints
- **Distributed clustering** — multi-node clusters with shard-based data distribution
- **Synchronous replication** — primary-replica replication over gRPC; writes acknowledged only after all in-sync replicas confirm
- **Scatter-gather search** — queries fan out across shards, results merged and returned
- **Crash recovery** — binary write-ahead log (WAL) with fsync-on-write durability
- **Zero external dependencies** — no JVM, no Zookeeper, just a single binary

## Getting Started

### Prerequisites

- Rust (2024 edition)
- Protobuf compiler (`protoc`)

### Single node

```bash
cargo run
```

```bash
curl http://localhost:9200/
```
```json
{"name": "ropensearch-node", "version": "0.1.0", "engine": "tantivy"}
```

### Multi-node cluster

```bash
# Terminal 1
./dev_cluster.sh 1    # HTTP 9200 · Transport 9300

# Terminal 2
./dev_cluster.sh 2    # HTTP 9201 · Transport 9301
```

### Configuration

Configure via `config/ropensearch.yml` or `ROPENSEARCH_*` environment variables:

| Option | Default | Description |
|--------|---------|-------------|
| `node_name` | `node-1` | Node identifier |
| `cluster_name` | `ropensearch` | Cluster name |
| `http_port` | `9200` | REST API port |
| `transport_port` | `9300` | gRPC transport port |
| `data_dir` | `./data` | Data storage directory |
| `seed_hosts` | `["127.0.0.1:9300"]` | Seed nodes for discovery |

## API Reference

### Indices

```bash
# Create an index
curl -X PUT 'http://localhost:9200/my-index' \
  -H 'Content-Type: application/json' \
  -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 1}}'

# Delete an index
curl -X DELETE 'http://localhost:9200/my-index'
```

### Documents

```bash
# Index a document
curl -X POST 'http://localhost:9200/my-index/_doc' \
  -H 'Content-Type: application/json' \
  -d '{"title": "Hello World", "tags": "rust search"}'

# Get a document
curl 'http://localhost:9200/my-index/_doc/{id}'

# Delete a document
curl -X DELETE 'http://localhost:9200/my-index/_doc/{id}'

# Bulk index
curl -X POST 'http://localhost:9200/my-index/_bulk' \
  -H 'Content-Type: application/json' \
  -d '[
    {"_doc_id": "doc-1", "_source": {"name": "Alice"}},
    {"_doc_id": "doc-2", "_source": {"name": "Bob"}}
  ]'
```

### Search

```bash
# Match all
curl 'http://localhost:9200/my-index/_search'

# Query string
curl 'http://localhost:9200/my-index/_search?q=rust'

# DSL query
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}}'
```

### Operations

```bash
curl -X POST 'http://localhost:9200/my-index/_refresh'   # Make recent writes searchable
curl -X POST 'http://localhost:9200/my-index/_flush'      # Fsync translog to disk
```

### Monitoring

```bash
curl 'http://localhost:9200/_cluster/health'    # Cluster health
curl 'http://localhost:9200/_cluster/state'     # Cluster state
curl 'http://localhost:9200/_cat/nodes'         # List nodes
curl 'http://localhost:9200/_cat/shards'        # List shards
curl 'http://localhost:9200/_cat/indices'       # List indices
```

> Append `?pretty` to any endpoint for formatted JSON.

## Replication

ROpenSearch implements synchronous primary-replica replication:

1. Client writes to the primary shard
2. Primary persists to its engine and WAL
3. Primary forwards the operation to all in-sync replicas via gRPC
4. Write is acknowledged only after all replicas confirm
5. Replica shards are lazily initialized on first replication request

## Testing

```bash
cargo test                                      # All tests
cargo test --lib                                # Unit tests
cargo test --test replication_integration        # Integration tests
```

Integration tests run entirely in-process — they spin up real gRPC servers with isolated temp directories. No external services needed.

## Project Structure

```
src/
├── api/           REST API handlers (Axum)
├── cluster/       Cluster state, membership, shard routing
├── config/        Configuration loading
├── engine/        Tantivy search engine wrapper
├── replication/   Primary → replica replication
├── shard/         Shard lifecycle management
├── transport/     gRPC client & server (tonic)
├── wal/           Write-ahead log
└── main.rs

proto/             gRPC service definitions
tests/             Integration tests
config/            Default configuration
```

## Roadmap

### Search & Query
- [ ] Pagination support (`from` / `size` parameters)
- [ ] Sort by field and `_score`
- [ ] Bool queries (`must`, `should`, `must_not`, `filter`)
- [ ] Range queries (`gt`, `gte`, `lt`, `lte`)
- [ ] Wildcard and prefix queries
- [ ] Return `_score` in search results
- [ ] Aggregations (terms, histogram, stats)

### Index Management
- [ ] Field mappings in `PUT /{index}` (explicit schema definition)
- [ ] Dynamic vs. strict mapping modes
- [ ] Update document API (`POST /{index}/_update/{id}`)
- [ ] Index aliases
- [ ] Index templates
- [ ] Dynamic settings updates (`number_of_replicas`, `refresh_interval`)
- [ ] Document versioning and optimistic concurrency control

### Cluster Reliability
- [ ] Leader election with quorum consensus (Raft)
- [ ] Automatic shard rebalancing across nodes
- [ ] Node failure detection with shard reassignment
- [ ] Replica promotion on primary failure
- [ ] Shard awareness (co-location prevention)
- [ ] Delayed allocation for rolling restarts

### Replication & Recovery
- [ ] Replica recovery (catch-up from primary after downtime)
- [ ] Sequence number checkpointing
- [ ] In-sync replica set (ISR) tracking
- [ ] Segment-level replication (ship segment files instead of individual docs)
- [ ] Slow replica detection and backpressure

### Transport & Resilience
- [ ] Connection pooling (reuse gRPC channels)
- [ ] Retry with exponential backoff
- [ ] Circuit breaker for unresponsive nodes
- [ ] TLS encryption for gRPC transport
- [ ] Adaptive request timeouts

### Storage & Durability
- [ ] Snapshot and restore
- [ ] Remote storage backends (S3, GCS, Azure Blob)
- [ ] Tiered storage (hot/warm/cold)
- [ ] Translog retention policies (size and time-based)
- [ ] Rolling translog segments

### Observability
- [ ] Prometheus metrics endpoint (`/_metrics`)
- [ ] Per-node and per-shard stats APIs
- [ ] Query latency histograms
- [ ] Index size and document count tracking
- [ ] OpenTelemetry tracing integration

### Security
- [ ] Basic authentication (username/password)
- [ ] Role-based access control (RBAC)
- [ ] TLS for HTTP API
- [ ] Encryption at rest
Apache-2.0
