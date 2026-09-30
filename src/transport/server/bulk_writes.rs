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
    let (status, error_type) = match error.code() {
        tonic::Code::AlreadyExists => (409, "version_conflict_engine_exception"),
        tonic::Code::InvalidArgument => (400, "mapper_parsing_exception"),
        tonic::Code::Aborted => (503, "shard_not_available_exception"),
        tonic::Code::ResourceExhausted
            if error
                .message()
                .starts_with(crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX) =>
        {
            (429, "version_map_capacity_exceeded")
        }
        _ => (500, "shard_failure"),
    };
    ShardBulkItemResponse {
        doc_id,
        status,
        error: error.message().to_string(),
        error_type: error_type.to_string(),
        ..Default::default()
    }
}

pub(super) async fn execute_ordered_bulk(
    service: &TransportService,
    request: ShardBulkRequest,
) -> Result<Response<ShardBulkResponse>, Status> {
    let mut results = Vec::with_capacity(request.documents_json.len());
    for (document_json, operation) in request.documents_json.into_iter().zip(request.operations) {
        let document: serde_json::Value = match serde_json::from_slice(&document_json) {
            Ok(document) => document,
            Err(error) => {
                results.push(failed_item(
                    String::new(),
                    Status::invalid_argument(error.to_string()),
                ));
                continue;
            }
        };
        let Some(doc_id) = document.get("_doc_id").and_then(serde_json::Value::as_str) else {
            results.push(failed_item(
                String::new(),
                Status::invalid_argument("bulk item has no _doc_id"),
            ));
            continue;
        };
        let doc_id = doc_id.to_string();
        let kind = ShardBulkOpKind::try_from(operation.kind)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let item = if kind == ShardBulkOpKind::Delete {
            match service
                .delete_doc(Request::new(ShardDeleteRequest {
                    index_name: request.index_name.clone(),
                    shard_id: request.shard_id,
                    doc_id: doc_id.clone(),
                    if_seq_no: operation.if_seq_no,
                    if_primary_term: operation.if_primary_term,
                }))
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
                            ..Default::default()
                        }
                    } else {
                        failed_item(doc_id, Status::internal(response.error))
                    }
                }
                Err(error) => failed_item(doc_id, error),
            }
        } else {
            let Some(source) = document.get("_source") else {
                results.push(failed_item(
                    doc_id,
                    Status::invalid_argument("bulk index item has no _source"),
                ));
                continue;
            };
            let payload_json =
                serde_json::to_vec(source).map_err(|error| Status::internal(error.to_string()))?;
            match service
                .index_doc(Request::new(ShardDocRequest {
                    index_name: request.index_name.clone(),
                    shard_id: request.shard_id,
                    doc_id: doc_id.clone(),
                    payload_json,
                    if_seq_no: operation.if_seq_no,
                    if_primary_term: operation.if_primary_term,
                    create_only: kind == ShardBulkOpKind::Create,
                }))
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
                            ..Default::default()
                        }
                    } else {
                        failed_item(doc_id, Status::internal(response.error))
                    }
                }
                Err(error) => failed_item(doc_id, error),
            }
        };
        results.push(item);
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
