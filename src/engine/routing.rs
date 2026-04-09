use crate::cluster::state::IndexMetadata;
use std::io::Cursor;

/// A deterministic routing function using Murmur3 (same algorithm as OpenSearch/Elasticsearch)
/// to determine which shard a specific document ID belongs to.
/// Unlike DefaultHasher, Murmur3 output is stable across Rust versions.
pub fn calculate_shard(document_id: &str, number_of_shards: u32) -> u32 {
    let hash128 = murmur3::murmur3_x86_128(&mut Cursor::new(document_id.as_bytes()), 0)
        .expect("murmur3 hashing from memory cursor cannot fail");
    // Use the lower 32 bits for shard routing
    let hash = hash128 as u32;
    hash % number_of_shards
}

/// Hash a string to a u64 key (used for USearch vector index keys).
pub fn hash_string(s: &str) -> u64 {
    let hash128 = murmur3::murmur3_x86_128(&mut Cursor::new(s.as_bytes()), 0)
        .expect("murmur3 hashing from memory cursor cannot fail");
    hash128 as u64
}

/// Given a document ID and the index metadata, determine exactly which Node ID should hold this document
pub fn route_document(document_id: &str, index_metadata: &IndexMetadata) -> Option<String> {
    if index_metadata.number_of_shards == 0 {
        return None;
    }

    let shard_id = calculate_shard(document_id, index_metadata.number_of_shards);
    index_metadata.primary_node(shard_id).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── calculate_shard ─────────────────────────────────────────────────

    #[test]
    fn shard_is_within_range() {
        for num_shards in [1, 2, 3, 5, 10, 100] {
            for i in 0..200 {
                let doc_id = format!("doc-{}", i);
                let shard = calculate_shard(&doc_id, num_shards);
                assert!(
                    shard < num_shards,
                    "shard {} out of range for {} shards",
                    shard,
                    num_shards
                );
            }
        }
    }

    #[test]
    fn same_id_always_routes_to_same_shard() {
        let s1 = calculate_shard("my-document", 5);
        let s2 = calculate_shard("my-document", 5);
        assert_eq!(s1, s2, "deterministic routing");
    }

    #[test]
    fn different_ids_distribute_across_shards() {
        let num_shards = 5;
        let mut seen = std::collections::HashSet::new();
        // With enough docs we should hit more than 1 shard
        for i in 0..100 {
            seen.insert(calculate_shard(&format!("doc-{}", i), num_shards));
        }
        assert!(seen.len() > 1, "documents should spread across shards");
    }

    #[test]
    fn single_shard_always_returns_zero() {
        for i in 0..50 {
            assert_eq!(calculate_shard(&format!("id-{}", i), 1), 0);
        }
    }

    // ── route_document ──────────────────────────────────────────────────

    #[test]
    fn route_document_returns_correct_node() {
        use crate::cluster::state::ShardRoutingEntry;
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-B".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        let meta = IndexMetadata {
            name: "test-index".into(),
            uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
            number_of_shards: 2,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        };

        let routed = route_document("some-doc", &meta);
        assert!(routed.is_some());
        let node = routed.unwrap();
        assert!(node == "node-A" || node == "node-B");
    }

    #[test]
    fn route_document_zero_shards_returns_none() {
        let meta = IndexMetadata {
            name: "empty".into(),
            uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
            number_of_shards: 0,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        };
        assert!(route_document("doc", &meta).is_none());
    }
}
