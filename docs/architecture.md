# FerrisSearch Architecture

```mermaid
classDiagram
    class SearchEngine {
        <<trait>>
        +add_document(doc_id, payload) String
        +bulk_add_documents(docs) Vec~String~
        +delete_document(doc_id) u64
        +get_document(doc_id) Option~Value~
        +refresh()
        +flush()
        +search(query_str) Vec~Value~
        +search_query(req) Vec~Value~
        +search_knn(field, vector, k) Vec~Value~
        +doc_count() u64
    }

    class CompositeEngine {
        -text: HotEngine
        -vector: RwLock~Option~VectorIndex~~
        -data_dir: PathBuf
        +new(data_dir, refresh_interval)
        +start_refresh_loop(Arc~Self~)
        +rebuild_vectors()
        -ensure_vector_index(dimensions)
        -index_vectors(doc_id, payload)
        -save_vectors()
    }

    class HotEngine {
        -index: tantivy::Index
        -reader: IndexReader
        -writer: Arc~RwLock~IndexWriter~~
        -field_registry: RwLock~FieldRegistry~
        -translog: Arc~Mutex~dyn WAL~~
        +new(data_dir, refresh_interval)
        +start_refresh_loop(Arc~Self~)
        -build_query(clause) Box~dyn Query~
        -execute_search(searcher, query, limit) Vec~Value~
        -replay_translog()
    }

    class VectorIndex {
        -index: usearch::Index
        -dimensions: usize
        -key_to_doc_id: RwLock~HashMap~
        +new(dimensions, metric)
        +open(path, dimensions, metric)
        +add(key, vector)
        +add_with_doc_id(doc_id, vector) u64
        +doc_id_for_key(key) Option~String~
        +remove(key)
        +search(query, k) Vec~u64~ Vec~f32~
        +save(path)
    }

    class ShardManager {
        -data_dir: PathBuf
        -refresh_interval: Duration
        -shards: RwLock~HashMap~ShardKey, Arc~dyn SearchEngine~~~
        +open_shard(index, shard_id) Arc~dyn SearchEngine~
        +get_shard(index, shard_id) Option
        +get_index_shards(index) Vec
        +close_index_shards(index)
    }

    class RaftInstance {
        +client_write(cmd)
        +add_learner(id, node, blocking)
        +change_membership(members, retain)
        +handle_transfer_leader(req)
        +is_leader() bool
        +current_leader() Option~u64~
    }

    class ClusterStateMachine {
        -state: Arc~RwLock~ClusterState~~
        +apply(entries)
        +state_handle() Arc~RwLock~ClusterState~~
    }

    class DiskLogStore {
        -db: Arc~Mutex~Database~~
        +open(path)
        +append(entries, callback)
        +save_vote(vote)
        +read_vote() Option~Vote~
    }

    class TransportService {
        -cluster_manager: Arc~ClusterManager~
        -shard_manager: Arc~ShardManager~
        -raft: Option~Arc~RaftInstance~~
        +join_cluster(req)
        +index_doc(req)
        +search_shard(req)
        +raft_vote(req)
        +raft_append_entries(req)
    }

    class AppState {
        +cluster_manager: Arc~ClusterManager~
        +shard_manager: Arc~ShardManager~
        +transport_client: TransportClient
        +raft: Option~Arc~RaftInstance~~
        +local_node_id: String
    }

    SearchEngine <|.. CompositeEngine : implements
    SearchEngine <|.. HotEngine : implements
    CompositeEngine *-- HotEngine : text search
    CompositeEngine *-- VectorIndex : vector search optional
    ShardManager o-- SearchEngine : manages shards
    TransportService o-- ShardManager : uses
    TransportService o-- RaftInstance : Raft RPCs
    AppState o-- ShardManager : uses
    AppState o-- RaftInstance : Raft writes
    RaftInstance --> ClusterStateMachine : applies entries
    RaftInstance --> DiskLogStore : persists log
    ClusterStateMachine --> ClusterState : owns state
```

## Data Flow

### Document Indexing
```
Client → HTTP API → Shard Routing → TransportService.index_doc()
                                         ↓
                                    CompositeEngine.add_document()
                                         ↓
                              ┌──────────┴──────────┐
                              ↓                     ↓
                     HotEngine (Tantivy)    VectorIndex (USearch)
                     - WAL write            - HNSW graph insert
                     - Tantivy buffer       - doc_id mapping
                              ↓                     ↓
                        Replication → replica shards via gRPC
```

### Search Query
```
Client → HTTP API → Scatter to all shards
                         ↓
              ┌──────────┴──────────┐
              ↓                     ↓
      Local shards            Remote shards (gRPC)
              ↓                     ↓
    CompositeEngine          TransportService
    .search_query()          .search_shard_dsl()
    .search_knn()
              ↓                     ↓
         Coordinator: merge results, sort by _score, apply from/size
              ↓
         JSON response
```

### Raft Consensus
```
Client write (CreateIndex, AddNode, etc.)
              ↓
      Raft leader: client_write(ClusterCommand)
              ↓
      Log replication → DiskLogStore (redb)
              ↓
      Majority ack → committed
              ↓
      ClusterStateMachine.apply() → ClusterState updated
              ↓
      All nodes see consistent state via shared Arc<RwLock<ClusterState>>
```
