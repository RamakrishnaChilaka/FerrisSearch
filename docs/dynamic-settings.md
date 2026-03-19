# Dynamic Settings

FerrisSearch supports modifying index-level settings at runtime without rebuilding or recreating the index. Changes propagate through Raft consensus and are reactively applied to running shard engines via `tokio::sync::watch` channels.

## Architecture

```
PUT /{index}/_settings (any node)
      │
      ├── (if leader) ──► Raft client_write(UpdateIndex)
      │                         │
      └── (if follower) ──► gRPC ForwardUpdateSettings ──► leader
                                │
                                ▼
                      State machine apply
                                │
                                ▼
                      ShardManager::apply_settings()
                                │
                                ▼
                      SettingsManager::update()
                                │
                      ┌─────────┴─────────┐
                      ▼                   ▼
              watch::Sender          (future channels)
              refresh_interval       translog_durability, etc.
                      │
                      ▼
              CompositeEngine
              refresh loop wakes up
              and adjusts interval
```

Each index has a `SettingsManager` that holds `watch::Sender<T>` channels. Consumers (the refresh loop, WAL, etc.) subscribe via `watch::Receiver<T>` and use `tokio::select!` to react when a value changes.

## Supported Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `index.refresh_interval_ms` | `u64 \| null` | `5000` | How often new documents become searchable (ms). Set to `null` to reset to default. |
| `index.number_of_replicas` | `u32` | — | Number of replica copies per shard. Increasing adds unassigned replicas; decreasing removes assigned ones. |

**Immutable settings** (rejected with `400 Bad Request`):
- `index.number_of_shards` — cannot be changed after index creation.

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
                "refresh_interval_ms": null
            }
        }
    }
}
```

A `null` value for `refresh_interval_ms` means the cluster default (5000ms) is in effect.

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

Settings updates can be sent to **any node** in the cluster. If the receiving node is not the Raft leader, it transparently forwards the request to the current master via the `UpdateSettings` gRPC RPC. The master applies the change through Raft, and log replication propagates it to all followers.

## Adding a New Reactive Setting

1. Add the field to `IndexSettings` in `src/cluster/state.rs`
2. Add a `watch::Sender<T>` field and `watch_*()` accessor to `SettingsManager` in `src/cluster/settings.rs`
3. In `SettingsManager::update()`, detect changes and call `send()` on the new channel
4. In the consumer (engine, WAL, etc.), subscribe via `watch_*()` and react in a `tokio::select!` loop
5. Add parsing to `update_index_settings` in `src/api/index.rs` and the gRPC handler in `src/transport/server.rs`
