# Dynamic Settings

FerrisSearch supports modifying selected index-level settings without rebuilding
or recreating the index. The authoritative metadata propagates through Raft.
Request-path nodes apply changed engine settings locally through
`tokio::sync::watch` channels.

> [!WARNING]
> Raft replication currently updates `ClusterState` on every node, but it does
> not itself notify every follower's already-open shard engines. The HTTP
> receiver and leader RPC handler update their local watchers; other followers
> consume the new settings when a shard is subsequently opened, but an
> already-open engine can retain its previous runtime value. Do not treat this
> path as cluster-wide reactive application until that gap has a multi-node
> regression.

## Architecture

```
PUT /{index}/_settings (any node)
      │
      ├── (if leader) ──► Raft client_write(UpdateIndex)
      │                         │
      └── (if follower) ──► gRPC ForwardUpdateSettings ──► leader
                                │
                                ▼
                      Raft state machine apply
                      (metadata on every node)
                                │
                                ▼
                 Request-path node applies locally
                      ShardManager::apply_settings()
                                │
                                ▼
                      SettingsManager::update()
                                │
                   ┌────────────┴────────────┐
                   ▼                         ▼
       refresh-interval watcher    flush-threshold watcher
```

Each locally opened index has a `SettingsManager` that holds
`watch::Sender<T>` channels. Engine refresh and auto-flush loops subscribe via
`watch::Receiver<T>`.

## Supported Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `index.refresh_interval_ms` | `u64 \| null` | `5000` | How often new documents become searchable (ms). Set to `null` to reset to default. |
| `index.flush_threshold_bytes` | `u64 \| null` | `536870912` | WAL size threshold for background flush. `0` disables auto-flush; `null` restores the default. |
| `index.number_of_replicas` | `u32` | — | Number of replica copies per shard. Increasing adds unassigned replicas; decreasing removes assigned ones. |

**Immutable settings** (rejected with `400 Bad Request`):
- `index.number_of_shards` — cannot be changed after index creation.
- `index.engine` — engine selection is fixed at index creation.

## API Reference

### Get Index Settings

Retrieve the current settings for an index.

```bash
GET /{index}/_settings
```

**Example:**

```bash
curl -s 'http://localhost:9200/movies/_settings' | python3 -m json.tool
```

**Response:**

```json
{
    "movies": {
        "settings": {
            "index": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "engine": "local_shards",
                "refresh_interval_ms": null,
                "flush_threshold_bytes": null,
                "dynamic": "false"
            }
        }
    }
}
```

A `null` refresh interval or flush threshold means its engine default is in
effect.

### Update Index Settings

Modify dynamic settings on a live index.

```bash
PUT /{index}/_settings
Content-Type: application/json

{
    "index": {
        "<setting>": <value>
    }
}
```

**Example — change refresh interval:**

```bash
curl -X PUT 'http://localhost:9200/movies/_settings' \
  -H 'Content-Type: application/json' \
  -d '{"index": {"refresh_interval_ms": 2000}}'
```

**Response:**

```json
{"acknowledged": true}
```

**Example — increase replicas:**

```bash
curl -X PUT 'http://localhost:9200/movies/_settings' \
  -H 'Content-Type: application/json' \
  -d '{"index": {"number_of_replicas": 2}}'
```

**Example — reset refresh interval to default:**

```bash
curl -X PUT 'http://localhost:9200/movies/_settings' \
  -H 'Content-Type: application/json' \
  -d '{"index": {"refresh_interval_ms": null}}'
```

**Example — multiple settings at once:**

```bash
curl -X PUT 'http://localhost:9200/movies/_settings' \
  -H 'Content-Type: application/json' \
  -d '{"index": {"refresh_interval_ms": 1000, "number_of_replicas": 3}}'
```

### Error Cases

**Index not found (404):**

```bash
curl -s 'http://localhost:9200/nonexistent/_settings'
```

```json
{
    "error": {
        "type": "index_not_found_exception",
        "reason": "no such index [nonexistent]"
    },
    "status": 404
}
```

**Attempt to change immutable setting (400):**

```bash
curl -X PUT 'http://localhost:9200/movies/_settings' \
  -H 'Content-Type: application/json' \
  -d '{"index": {"number_of_shards": 5}}'
```

```json
{
    "error": {
        "type": "illegal_argument_exception",
        "reason": "index.number_of_shards is immutable and cannot be changed after index creation"
    },
    "status": 400
}
```

## Forwarding Behavior

Settings updates can be sent to **any node** in the cluster. If the receiving
node is not the Raft leader, it transparently forwards the request to the
leader through the `UpdateSettings` gRPC RPC. The leader commits the metadata
through Raft, and log replication propagates it to all followers. As noted
above, that metadata propagation is currently broader than the local
watch-channel notification path.

## Adding a New Reactive Setting

1. Add the field to `IndexSettings` in `src/cluster/state.rs`
2. Add a `watch::Sender<T>` field and `watch_*()` accessor to `SettingsManager` in `src/cluster/settings.rs`
3. In `SettingsManager::update()`, detect changes and call `send()` on the new channel
4. In the consumer (engine, WAL, etc.), subscribe via `watch_*()` and react in a `tokio::select!` loop
5. Add parsing to `update_index_settings` in `src/api/index/mod.rs` and the gRPC handler in `src/transport/server/mod.rs`
6. Preserve the field through Raft/transport snapshots and prove every node
   applies the committed value to already-open engines
