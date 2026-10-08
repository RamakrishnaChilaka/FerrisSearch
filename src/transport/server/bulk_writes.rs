use super::*;

pub(super) fn index_batch_results(
    receipt: &crate::engine::BulkWriteReceipt,
) -> anyhow::Result<Vec<ShardBulkItemResponse>> {
    receipt.last_seq_no()?;
    receipt
        .doc_ids
        .iter()
        .zip(&receipt.created)
        .enumerate()
        .map(|(offset, (doc_id, created))| {
            let seq_no = receipt
                .start_seq_no
                .and_then(|start| start.checked_add(offset as u64))
                .ok_or_else(|| anyhow::anyhow!("bulk item has no assigned sequence"))?;
            Ok(ShardBulkItemResponse {
                doc_id: doc_id.clone(),
                status: if *created { 201 } else { 200 },
                result: if *created { "created" } else { "updated" }.to_string(),
                seq_no: Some(seq_no),
                primary_term: Some(receipt.primary_term),
                ..Default::default()
            })
        })
        .collect()
}

fn failed_item(doc_id: String, error: Status) -> ShardBulkItemResponse {
    WriteFailure::from_rpc_status(error).bulk_item(doc_id)
}

fn failed_response(
    doc_id: String,
    details: Option<WriteFailureDetails>,
    reason: &str,
    seq_no: Option<u64>,
    primary_term: Option<u64>,
) -> ShardBulkItemResponse {
    let failure = match WriteFailure::response_failure(details, reason, seq_no, primary_term) {
        Ok(failure) if failure.last_seq_no.is_none() => failure,
        Ok(_) => WriteFailure::indeterminate(
            "single bulk mutation returned a bulk failure range",
            None,
            None,
            None,
        ),
        Err(error) => WriteFailure::indeterminate(
            format!("invalid bulk mutation failure response: {error:#}"),
            None,
            None,
            None,
        ),
    };
    failure.bulk_item(doc_id)
}

pub(super) async fn execute_ordered_bulk(
    service: &TransportService,
    request: ShardBulkRequest,
    refresh_deadline: Option<crate::transport::refresh_deadline::RefreshDeadline>,
) -> Result<Response<ShardBulkResponse>, Status> {
    let index_uuid = if refresh_deadline.is_some() {
        service
            .cluster_manager
            .get_state()
            .indices
            .get(&request.index_name)
            .map(|metadata| metadata.uuid.to_string())
    } else {
        None
    };
    let mut results = Vec::with_capacity(request.documents_json.len());
    for (document_json, operation) in request.documents_json.into_iter().zip(request.operations) {
        let document: serde_json::Value = match serde_json::from_slice(&document_json) {
            Ok(document) => document,
            Err(error) => {
                results.push(failed_item(
                    String::new(),
                    WriteFailure::rejected(400, "mapper_parsing_exception", error.to_string())
                        .into_status(),
                ));
                continue;
            }
        };
        let Some(doc_id) = document.get("_doc_id").and_then(serde_json::Value::as_str) else {
            results.push(failed_item(
                String::new(),
                WriteFailure::rejected(400, "mapper_parsing_exception", "bulk item has no _doc_id")
                    .into_status(),
            ));
            continue;
        };
        let doc_id = doc_id.to_string();
        let kind = match ShardBulkOpKind::try_from(operation.kind) {
            Ok(kind) => kind,
            Err(error) => {
                results.push(
                    WriteFailure::rejected(400, "mapper_parsing_exception", error.to_string())
                        .bulk_item(doc_id),
                );
                continue;
            }
        };
        let item = if kind == ShardBulkOpKind::Delete {
            match service
                .delete_doc(service.transport_client.forwarding_request(
                    ShardDeleteRequest {
                        index_name: request.index_name.clone(),
                        shard_id: request.shard_id,
                        doc_id: doc_id.clone(),
                        if_seq_no: operation.if_seq_no,
                        if_primary_term: operation.if_primary_term,
                        refresh: false,
                    },
                    &request.index_name,
                    Some((request.shard_id, &service.local_node_id)),
                ))
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    if response.success {
                        ShardBulkItemResponse {
                            doc_id,
                            status: if response.deleted > 0 { 200 } else { 404 },
                            result: if response.deleted > 0 {
                                "deleted"
                            } else {
                                "not_found"
                            }
                            .to_string(),
                            seq_no: response.seq_no,
                            primary_term: response.primary_term,
                            write_refresh: response.write_refresh,
                            ..Default::default()
                        }
                    } else {
                        failed_response(
                            doc_id,
                            response.failure,
                            &response.error,
                            response.seq_no,
                            response.primary_term,
                        )
                    }
                }
                Err(error) => failed_item(doc_id, error),
            }
        } else {
            let Some(source) = document.get("_source") else {
                results.push(failed_item(
                    doc_id,
                    WriteFailure::rejected(
                        400,
                        "mapper_parsing_exception",
                        "bulk index item has no _source",
                    )
                    .into_status(),
                ));
                continue;
            };
            let payload_json = match serde_json::to_vec(source) {
                Ok(payload) => payload,
                Err(error) => {
                    results.push(
                        WriteFailure::not_executed(format!(
                            "failed to encode bulk source before dispatch: {error}"
                        ))
                        .bulk_item(doc_id),
                    );
                    continue;
                }
            };
            match service
                .index_doc(service.transport_client.forwarding_request(
                    ShardDocRequest {
                        index_name: request.index_name.clone(),
                        shard_id: request.shard_id,
                        doc_id: doc_id.clone(),
                        payload_json,
                        if_seq_no: operation.if_seq_no,
                        if_primary_term: operation.if_primary_term,
                        create_only: kind == ShardBulkOpKind::Create,
                        index_uuid: None,
                        refresh: false,
                    },
                    &request.index_name,
                    Some((request.shard_id, &service.local_node_id)),
                ))
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    if response.success {
                        ShardBulkItemResponse {
                            doc_id,
                            status: if response.created { 201 } else { 200 },
                            result: if response.created {
                                "created"
                            } else {
                                "updated"
                            }
                            .to_string(),
                            seq_no: response.seq_no,
                            primary_term: response.primary_term,
                            write_refresh: response.write_refresh,
                            ..Default::default()
                        }
                    } else {
                        failed_response(
                            doc_id,
                            response.failure,
                            &response.error,
                            response.seq_no,
                            response.primary_term,
                        )
                    }
                }
                Err(error) => failed_item(doc_id, error),
            }
        };
        results.push(item);
    }
    if let Some(deadline) = refresh_deadline
        && let Some(term) = results
            .iter()
            .rev()
            .find(|item| item.error.is_empty() && item.seq_no.is_some())
            .and_then(|item| item.primary_term)
    {
        let current = service.cluster_manager.get_state();
        let allocation = current
            .shard_allocation_id(
                &request.index_name,
                request.shard_id,
                &service.local_node_id,
            )
            .filter(|allocation| *allocation > 0);
        let refresh = async {
            let uuid = index_uuid.ok_or_else(|| {
                Status::failed_precondition("ordered bulk refresh UUID is missing")
            })?;
            let allocation = allocation.ok_or_else(|| {
                Status::failed_precondition("ordered bulk refresh allocation is missing")
            })?;
            service
                .refresh_primary_shard_writes(
                    ShardCopyRefreshRequest {
                        index_name: request.index_name.clone(),
                        index_uuid: uuid,
                        shard_id: request.shard_id,
                        primary_node_id: service.local_node_id.clone(),
                        primary_term: Some(term),
                        target_allocation_id: Some(allocation),
                    },
                    deadline,
                )
                .await
        }
        .await;
        let report = match refresh {
            Ok(report) => report,
            Err(error) => {
                tracing::error!(
                    index = request.index_name, shard = request.shard_id,
                    error = %error,
                    "Ordered bulk refresh failed after writes acknowledged"
                );
                ShardWriteRefreshResult {
                    copies: vec![crate::transport::write_refresh::copy_refresh_failure(
                        &service.local_node_id,
                        allocation,
                        true,
                        error.to_string(),
                    )],
                }
            }
        };
        for item in &mut results {
            if item.error.is_empty() {
                item.write_refresh = Some(report.clone());
            }
        }
    }
    crate::metrics::BULK_DOCS_TOTAL
        .inc_by(results.iter().filter(|item| item.error.is_empty()).count() as u64);
    Ok(Response::new(ShardBulkResponse {
        success: true,
        doc_ids: results.iter().map(|item| item.doc_id.clone()).collect(),
        results,
        ..Default::default()
    }))
}
