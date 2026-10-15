use super::proto::{ShardWriteAcknowledgement, shard_write_copy_result::Outcome};
use serde_json::{Value, json};
use std::collections::HashSet;

pub(crate) fn validate_write_acknowledgement<'a>(
    result: Option<&'a ShardWriteAcknowledgement>,
    expected_primary: Option<&str>,
) -> anyhow::Result<&'a ShardWriteAcknowledgement> {
    let result = result.ok_or_else(|| {
        anyhow::anyhow!("acknowledged write is missing its data-copy acknowledgement report")
    })?;
    if result.copies.is_empty() {
        anyhow::bail!("write acknowledgement has no data-copy results");
    }
    let mut nodes = HashSet::new();
    let mut primaries = 0;
    for copy in &result.copies {
        if copy.node_id.is_empty() || copy.allocation_id == 0 || !nodes.insert(&copy.node_id) {
            anyhow::bail!("write acknowledgement has missing or duplicate copy identity");
        }
        match &copy.outcome {
            Some(Outcome::Acknowledged(true)) => {}
            Some(Outcome::Error(reason)) if !copy.primary && !reason.is_empty() => {}
            _ => anyhow::bail!("write acknowledgement has an invalid data-copy outcome"),
        }
        if copy.primary {
            if expected_primary.is_some_and(|node| copy.node_id != node) {
                anyhow::bail!("write acknowledgement has inconsistent primary identity");
            }
            primaries += 1;
        } else if expected_primary.is_some_and(|node| copy.node_id == node) {
            anyhow::bail!("write acknowledgement identifies the primary as a replica");
        }
    }
    if primaries != 1 {
        anyhow::bail!("write acknowledgement must contain exactly one acknowledged primary");
    }
    Ok(result)
}

pub(crate) fn add_write_acknowledgement_to_response(
    response: &mut Value,
    index: &str,
    shard: u32,
    result: &ShardWriteAcknowledgement,
) {
    let failures = result
        .copies
        .iter()
        .filter_map(|copy| match &copy.outcome {
            Some(Outcome::Error(reason)) => Some(json!({
                "index": index,
                "shard": shard,
                "node": copy.node_id,
                "allocation_id": copy.allocation_id,
                "primary": copy.primary,
                "reason": {"type": "replication_exception", "reason": reason}
            })),
            _ => None,
        })
        .collect::<Vec<_>>();
    response["_shards"] = json!({
        "total": result.copies.len(),
        "successful": result.copies.len() - failures.len(),
        "failed": failures.len(),
    });
    if !failures.is_empty() {
        response["_shards"]["failures"] = json!(failures);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::proto::ShardWriteCopyResult;

    fn copy(node: &str, primary: bool) -> ShardWriteCopyResult {
        ShardWriteCopyResult {
            node_id: node.into(),
            allocation_id: 7,
            primary,
            outcome: Some(Outcome::Acknowledged(true)),
        }
    }

    #[test]
    fn write_acknowledgement_wire_requires_complete_unique_exact_copy_results() {
        let primary = copy("primary", true);
        let replica = copy("replica", false);
        let valid = ShardWriteAcknowledgement {
            copies: vec![primary.clone(), replica.clone()],
        };
        validate_write_acknowledgement(Some(&valid), Some("primary")).unwrap();
        assert!(validate_write_acknowledgement(None, Some("primary")).is_err());
        for copies in [
            vec![],
            vec![replica.clone()],
            vec![copy("wrong-primary", true)],
            vec![primary.clone(), primary.clone()],
            vec![primary.clone(), copy("other-primary", true)],
            vec![primary.clone(), replica.clone(), replica.clone()],
            vec![ShardWriteCopyResult {
                allocation_id: 0,
                ..primary.clone()
            }],
            vec![ShardWriteCopyResult {
                node_id: String::new(),
                ..primary.clone()
            }],
            vec![ShardWriteCopyResult {
                outcome: None,
                ..primary.clone()
            }],
            vec![ShardWriteCopyResult {
                outcome: Some(Outcome::Acknowledged(false)),
                ..primary.clone()
            }],
            vec![ShardWriteCopyResult {
                outcome: Some(Outcome::Error("failed".into())),
                ..primary.clone()
            }],
            vec![
                primary.clone(),
                ShardWriteCopyResult {
                    outcome: Some(Outcome::Error(String::new())),
                    ..replica.clone()
                },
            ],
        ] {
            assert!(
                validate_write_acknowledgement(
                    Some(&ShardWriteAcknowledgement { copies }),
                    Some("primary")
                )
                .is_err()
            );
        }
    }

    #[test]
    fn write_acknowledgement_reports_real_data_counts_without_overwriting_visibility() {
        let result = ShardWriteAcknowledgement {
            copies: vec![
                copy("primary", true),
                copy("replica", false),
                ShardWriteCopyResult {
                    outcome: Some(Outcome::Error("lost proof; committed exclusion".into())),
                    ..copy("excluded", false)
                },
            ],
        };
        validate_write_acknowledgement(Some(&result), Some("primary")).unwrap();
        let mut response = json!({"_refresh": {"total": 2, "successful": 1, "failed": 1}});
        add_write_acknowledgement_to_response(&mut response, "idx", 0, &result);
        assert_eq!(response["_shards"]["total"], 3);
        assert_eq!(response["_shards"]["successful"], 2);
        assert_eq!(response["_shards"]["failed"], 1);
        assert_eq!(response["_shards"]["failures"][0]["node"], "excluded");
        assert_eq!(response["_shards"]["failures"][0]["allocation_id"], 7);
        assert_eq!(response["_refresh"]["failed"], 1);
    }
}
