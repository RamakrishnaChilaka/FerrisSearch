use super::proto::{
    ShardCopyRefreshRequest, ShardCopyRefreshResult, ShardWriteRefreshResult,
    shard_copy_refresh_result::Outcome,
};
use serde_json::{Value, json};
use std::collections::HashSet;

pub(crate) fn validate_refresh_request(request: &ShardCopyRefreshRequest) -> anyhow::Result<()> {
    if request.index_name.is_empty()
        || request.index_uuid.is_empty()
        || request.primary_node_id.is_empty()
        || request.primary_term.is_none_or(|term| term == 0)
        || request
            .target_allocation_id
            .is_none_or(|allocation| allocation == 0)
    {
        anyhow::bail!(
            "shard-copy refresh requires index, UUID, primary, term, and allocation identity"
        );
    }
    Ok(())
}

fn validate_copy_result(result: &ShardCopyRefreshResult) -> anyhow::Result<()> {
    if result.node_id.is_empty() || result.allocation_id == 0 {
        anyhow::bail!("shard-copy refresh response is missing its copy identity");
    }
    match &result.outcome {
        Some(Outcome::Refreshed(_)) => Ok(()),
        Some(Outcome::Error(error)) if !error.is_empty() => Ok(()),
        _ => anyhow::bail!("shard-copy refresh response is missing a valid outcome"),
    }
}

pub(crate) fn validate_refresh_copy_response(
    result: &ShardCopyRefreshResult,
    node_id: &str,
    allocation_id: u64,
    primary: bool,
) -> anyhow::Result<()> {
    validate_copy_result(result)?;
    if result.node_id != node_id
        || result.allocation_id != allocation_id
        || result.primary != primary
    {
        anyhow::bail!("shard-copy refresh response has inconsistent copy identity");
    }
    Ok(())
}

pub(crate) fn validate_write_refresh_response(
    result: Option<&ShardWriteRefreshResult>,
    requested: bool,
    primary_node: &str,
) -> anyhow::Result<()> {
    let Some(result) = result else {
        if requested {
            anyhow::bail!("acknowledged write response is missing its requested refresh results");
        }
        return Ok(());
    };
    if !requested {
        anyhow::bail!("write response contains unrequested refresh results");
    }
    if result.copies.is_empty() {
        anyhow::bail!("write refresh response has no copy results");
    }
    let mut nodes = HashSet::new();
    let mut primaries = 0;
    for copy in &result.copies {
        validate_copy_result(copy)?;
        if !nodes.insert(&copy.node_id) {
            anyhow::bail!("write refresh response contains duplicate copy results");
        }
        if copy.primary {
            if copy.node_id != primary_node {
                anyhow::bail!("write refresh response has inconsistent primary identity");
            }
            primaries += 1;
        } else if copy.node_id == primary_node {
            anyhow::bail!("write refresh response identifies the primary as a replica");
        }
    }
    if primaries != 1 {
        anyhow::bail!("write refresh response must contain exactly one primary result");
    }
    Ok(())
}

pub(crate) fn add_write_refresh_to_response(
    response: &mut Value,
    index_name: &str,
    shard_id: u32,
    result: &ShardWriteRefreshResult,
) {
    let failures = result
        .copies
        .iter()
        .filter_map(|copy| match &copy.outcome {
            Some(Outcome::Error(error)) => Some(json!({
                "index": index_name,
                "shard": shard_id,
                "node": copy.node_id,
                "allocation_id": copy.allocation_id,
                "primary": copy.primary,
                "reason": {"type": "refresh_exception", "reason": error}
            })),
            _ => None,
        })
        .collect::<Vec<_>>();
    response["_shards"] = json!({
        "total": result.copies.len(),
        "successful": result.copies.len() - failures.len(),
        "failed": failures.len()
    });
    if !failures.is_empty() {
        response["_shards"]["failures"] = json!(failures);
    }
    response["forced_refresh"] = json!(
        result
            .copies
            .iter()
            .any(|copy| { copy.primary && matches!(copy.outcome, Some(Outcome::Refreshed(_))) })
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::proto::Empty;
    use prost::Message;

    fn copy(node: &str, primary: bool) -> ShardCopyRefreshResult {
        ShardCopyRefreshResult {
            node_id: node.into(),
            allocation_id: 9,
            primary,
            outcome: Some(Outcome::Refreshed(Empty {})),
        }
    }

    #[test]
    fn refresh_copy_wire_rejects_missing_or_inconsistent_identity_and_outcome() {
        let valid = copy("replica", false);
        validate_refresh_copy_response(&valid, "replica", 9, false).unwrap();
        for invalid in [
            ShardCopyRefreshResult {
                node_id: String::new(),
                ..valid.clone()
            },
            ShardCopyRefreshResult {
                allocation_id: 0,
                ..valid.clone()
            },
            ShardCopyRefreshResult {
                outcome: None,
                ..valid.clone()
            },
            ShardCopyRefreshResult {
                outcome: Some(Outcome::Error(String::new())),
                ..valid.clone()
            },
            ShardCopyRefreshResult {
                primary: true,
                ..valid.clone()
            },
            ShardCopyRefreshResult {
                node_id: "other".into(),
                ..valid.clone()
            },
            ShardCopyRefreshResult {
                allocation_id: 10,
                ..valid.clone()
            },
        ] {
            assert!(validate_refresh_copy_response(&invalid, "replica", 9, false).is_err());
        }
    }

    #[test]
    fn refresh_write_wire_requires_requested_unique_primary_and_complete_results() {
        let valid = ShardWriteRefreshResult {
            copies: vec![copy("primary", true), copy("replica", false)],
        };
        validate_write_refresh_response(Some(&valid), true, "primary").unwrap();
        validate_write_refresh_response(None, false, "primary").unwrap();
        assert!(validate_write_refresh_response(None, true, "primary").is_err());
        assert!(validate_write_refresh_response(Some(&valid), false, "primary").is_err());
        for invalid in [
            ShardWriteRefreshResult { copies: vec![] },
            ShardWriteRefreshResult {
                copies: vec![copy("replica", false)],
            },
            ShardWriteRefreshResult {
                copies: vec![copy("wrong-primary", true)],
            },
            ShardWriteRefreshResult {
                copies: vec![copy("primary", false)],
            },
            ShardWriteRefreshResult {
                copies: vec![copy("primary", true), copy("primary", true)],
            },
            ShardWriteRefreshResult {
                copies: vec![copy("primary", true), copy("replica", true)],
            },
            ShardWriteRefreshResult {
                copies: vec![
                    copy("primary", true),
                    copy("replica", false),
                    copy("replica", false),
                ],
            },
            ShardWriteRefreshResult {
                copies: vec![
                    copy("primary", true),
                    ShardCopyRefreshResult {
                        outcome: None,
                        ..copy("replica", false)
                    },
                ],
            },
        ] {
            assert!(validate_write_refresh_response(Some(&invalid), true, "primary").is_err());
        }
    }

    #[test]
    fn refresh_request_wire_requires_positive_identity_fields() {
        let valid = ShardCopyRefreshRequest {
            index_name: "idx".into(),
            index_uuid: "uuid".into(),
            shard_id: 0,
            primary_node_id: "primary".into(),
            primary_term: Some(3),
            target_allocation_id: Some(9),
        };
        validate_refresh_request(&valid).unwrap();
        for invalid in [
            ShardCopyRefreshRequest {
                index_name: String::new(),
                ..valid.clone()
            },
            ShardCopyRefreshRequest {
                index_uuid: String::new(),
                ..valid.clone()
            },
            ShardCopyRefreshRequest {
                primary_node_id: String::new(),
                ..valid.clone()
            },
            ShardCopyRefreshRequest {
                primary_term: None,
                ..valid.clone()
            },
            ShardCopyRefreshRequest {
                primary_term: Some(0),
                ..valid.clone()
            },
            ShardCopyRefreshRequest {
                target_allocation_id: None,
                ..valid.clone()
            },
            ShardCopyRefreshRequest {
                target_allocation_id: Some(0),
                ..valid.clone()
            },
        ] {
            assert!(validate_refresh_request(&invalid).is_err());
        }
        let decoded = ShardCopyRefreshRequest::decode(valid.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, valid);
    }

    #[test]
    fn refresh_results_roundtrip_preserves_failures_and_write_acknowledgement() {
        let result = ShardWriteRefreshResult {
            copies: vec![
                copy("primary", true),
                ShardCopyRefreshResult {
                    outcome: Some(Outcome::Error("reader open failed: disk I/O".into())),
                    ..copy("replica", false)
                },
            ],
        };
        let decoded = ShardWriteRefreshResult::decode(result.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, result);
        validate_write_refresh_response(Some(&decoded), true, "primary").unwrap();
        let mut response =
            json!({"status": 201, "result": "created", "_seq_no": 0, "_primary_term": 3});
        add_write_refresh_to_response(&mut response, "idx", 2, &decoded);
        assert_eq!(response["status"], 201);
        assert_eq!(response["result"], "created");
        assert_eq!(response["_seq_no"], 0);
        assert_eq!(response["_primary_term"], 3);
        assert!(response.get("error").is_none());
        assert_eq!(response["_shards"]["total"], 2);
        assert_eq!(response["_shards"]["successful"], 1);
        assert_eq!(response["_shards"]["failed"], 1);
        assert_eq!(response["_shards"]["failures"][0]["node"], "replica");
        assert_eq!(
            response["_shards"]["failures"][0]["reason"]["reason"],
            "reader open failed: disk I/O"
        );
        assert_eq!(response["forced_refresh"], true);
    }

    #[test]
    fn refresh_primary_failure_does_not_claim_forced_refresh() {
        let result = ShardWriteRefreshResult {
            copies: vec![
                ShardCopyRefreshResult {
                    outcome: Some(Outcome::Error("primary reader failed".into())),
                    ..copy("primary", true)
                },
                copy("replica", false),
            ],
        };
        let mut response = json!({"result": "deleted", "_seq_no": 1, "_primary_term": 3});
        add_write_refresh_to_response(&mut response, "idx", 0, &result);
        assert_eq!(response["result"], "deleted");
        assert_eq!(response["_shards"]["failed"], 1);
        assert_eq!(response["_shards"]["failures"][0]["primary"], true);
        assert_eq!(response["forced_refresh"], false);
    }
}
