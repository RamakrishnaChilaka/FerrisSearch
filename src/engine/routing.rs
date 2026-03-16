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

/// Given a document ID and the index metadata, determine exactly which Node ID should hold this document
pub fn route_document(document_id: &str, index_metadata: &IndexMetadata) -> Option<String> {
    if index_metadata.number_of_shards == 0 {
        return None;
    }

    let shard_id = calculate_shard(document_id, index_metadata.number_of_shards);
    index_metadata.shards.get(&shard_id).cloned()
}
