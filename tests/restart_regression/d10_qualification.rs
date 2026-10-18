use super::*;
use std::io::Write;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RACING_DOCUMENTS: usize = 4;

#[derive(serde::Serialize)]
struct ExpectedDocument {
    source: Option<Value>,
    receipt: Value,
}

fn record_event(output: &mut fs::File, phase: &str, observation: Value) -> Result<()> {
    serde_json::to_writer(
        &mut *output,
        &json!({
            "phase": phase,
            "observation": observation,
        }),
    )?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

async fn discard_write_response(
    harness: &RestartClusterHarness,
    coordinator: &NodeConfig,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let payload = body
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()?
        .unwrap_or_default();
    let mut wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    ).into_bytes();
    wire.extend_from_slice(&payload);
    let expected_wire = wire.clone();
    let mut upstream = harness
        .client
        .request(method, format!("{}{path}", coordinator.base_url()));
    if let Some(body) = body {
        upstream = upstream.json(&body);
    }
    let upstream = upstream.build()?;
    let client = harness.client.clone();
    let mut proxy = tokio::task::JoinSet::new();
    proxy.spawn(async move {
        let (mut ingress, _) = listener.accept().await?;
        let mut submitted = vec![0; expected_wire.len()];
        ingress.read_exact(&mut submitted).await?;
        anyhow::ensure!(
            submitted == expected_wire,
            "loss proxy received a different request"
        );
        let response = client.execute(upstream).await?;
        let status = response.status();
        let receipt = response.json::<Value>().await?;
        // The oracle sees the backend receipt; the submitting client receives no response bytes.
        ingress.shutdown().await?;
        Ok::<_, anyhow::Error>((status, receipt))
    });
    let (status, receipt) = tokio::time::timeout(READY_TIMEOUT, async {
        let mut caller = tokio::net::TcpStream::connect(address).await?;
        caller.write_all(&wire).await?;
        let mut received = Vec::new();
        caller.read_to_end(&mut received).await?;
        anyhow::ensure!(
            received.is_empty(),
            "loss proxy leaked a response to the client"
        );
        proxy
            .join_next()
            .await
            .context("loss proxy did not finish")?
            .context("loss proxy task panicked")?
    })
    .await
    .context("lost-response exchange exceeded its watchdog")??;
    anyhow::ensure!(
        status == StatusCode::OK,
        "backend write failed: {status} {receipt}"
    );
    Ok(receipt)
}

async fn assert_oracle(
    harness: &mut RestartClusterHarness,
    oracle: &BTreeMap<String, ExpectedDocument>,
    index_uuid: &str,
) -> Result<()> {
    let replicas = harness.nodes.len() - 1;
    harness
        .wait_for_in_sync_replicas_with_unassigned(replicas, 2 - u32::try_from(replicas)?)
        .await?;
    for (doc_id, expected) in oracle {
        if let Some(source) = &expected.source {
            harness
                .assert_document_copies(doc_id, source, &expected.receipt)
                .await?;
        } else {
            let (status, document) = harness.get_document(doc_id).await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{doc_id}: {document}");
            assert_eq!(document["found"], false, "{doc_id}: {document}");
            for node in &harness.nodes {
                let copy = get_local_document_response(&node.config, doc_id).await?;
                assert!(!copy.found, "{} resurrected {doc_id}", node.config.name);
                assert_eq!(
                    copy.index_uuid, index_uuid,
                    "{}: {doc_id}",
                    node.config.name
                );
            }
        }
    }
    Ok(())
}

async fn race_updates_and_deletes(
    harness: &mut RestartClusterHarness,
    phase: u64,
    oracle: &mut BTreeMap<String, ExpectedDocument>,
    output: &mut fs::File,
) -> Result<()> {
    for document in 0..RACING_DOCUMENTS {
        let id = format!("race-{document}");
        if oracle[&id].source.is_none() {
            let source = expected_document(document + 100);
            let receipt = harness.put_document(&id, source.clone()).await?;
            oracle.insert(
                id,
                ExpectedDocument {
                    source: Some(source),
                    receipt,
                },
            );
        }
    }
    let before = harness.local_sequence_states().await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(RACING_DOCUMENTS * 2));
    let mut requests = Vec::new();
    for document in 0..RACING_DOCUMENTS {
        let id = format!("race-{document}");
        let expected = &oracle[&id];
        let query = condition_query(&expected.receipt)?;
        let actions = match document {
            0 => [false, false],
            1 => [true, true],
            _ => [false, true],
        };
        for (contender, delete) in actions.into_iter().enumerate() {
            let mut updated = expected
                .source
                .clone()
                .context("racing document is absent")?;
            updated["n"] = json!(phase * 100 + (document * 2 + contender) as u64 + 1000);
            updated["payload"] = json!(format!(
                "phase-{phase}-document-{document}-contender-{contender}"
            ));
            let coordinator = &harness.nodes[(document * 2 + contender) % harness.nodes.len()];
            let endpoint = if delete { "_doc" } else { "_update" };
            let path = format!("/{INDEX_NAME}/{endpoint}/{id}?{query}");
            let method = if delete { Method::DELETE } else { Method::POST };
            let source = (!delete).then(|| updated.clone());
            let body = (!delete).then(|| {
                json!({"doc": {
                    "n": updated["n"],
                    "payload": updated["payload"],
                }})
            });
            let request = harness
                .client
                .request(method, format!("{}{path}", coordinator.config.base_url()));
            let request = match body {
                Some(body) => request.json(&body),
                None => request,
            };
            let barrier = barrier.clone();
            let id = id.clone();
            requests.push(async move {
                barrier.wait().await;
                let response = request.send().await?;
                let status = response.status();
                let receipt = response.json::<Value>().await?;
                Ok::<_, anyhow::Error>((id, source, status, receipt))
            });
        }
    }
    let results = futures::future::try_join_all(requests).await?;
    let mut winners = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    let mut update_winners = 0;
    let mut delete_winners = 0;
    for (id, source, status, receipt) in results {
        record_event(
            output,
            "conditional_response",
            json!({
                "round": phase, "id": id, "source": source,
                "status": status.as_u16(), "receipt": receipt,
            }),
        )?;
        match status {
            StatusCode::OK => {
                assert!(
                    winners.insert(id.clone()),
                    "multiple conditional winners for {id}"
                );
                assert!(
                    sequences.insert(
                        receipt["_seq_no"]
                            .as_u64()
                            .context("winner has no sequence")?
                    )
                );
                assert_eq!(
                    receipt["_version"].as_u64(),
                    oracle[&id].receipt["_version"]
                        .as_u64()
                        .map(|version| version + 1),
                    "{receipt}"
                );
                assert_eq!(
                    receipt["result"],
                    if source.is_some() {
                        "updated"
                    } else {
                        "deleted"
                    }
                );
                assert!(
                    receipt["_shards"]["successful"]
                        .as_u64()
                        .is_some_and(|copies| copies >= 2),
                    "{receipt}"
                );
                if source.is_some() {
                    update_winners += 1;
                } else {
                    delete_winners += 1;
                }
                oracle.insert(id, ExpectedDocument { source, receipt });
            }
            StatusCode::CONFLICT => assert_eq!(
                receipt["error"]["type"], "version_conflict_engine_exception",
                "{receipt}"
            ),
            _ => bail!("unexpected conditional result for {id}: {status} {receipt}"),
        }
    }
    assert_eq!(winners.len(), RACING_DOCUMENTS);
    assert!(update_winners > 0, "no concurrent update succeeded");
    assert!(delete_winners > 0, "no concurrent delete succeeded");
    for (node, state) in harness.local_sequence_states().await? {
        assert_eq!(
            state.max_seq_no,
            before[&node]
                .max_seq_no
                .map(|sequence| sequence + RACING_DOCUMENTS as u64),
            "conditional losers allocated sequence state on {node}"
        );
    }
    Ok(())
}

async fn retry_lost_responses(
    harness: &RestartClusterHarness,
    lost: &[(Method, String, Option<Value>, Value)],
    output: &mut fs::File,
) -> Result<()> {
    let before = harness
        .local_sequence_states()
        .await
        .context("probe every open copy before retrying lost responses")?;
    for (method, path, body, original) in lost {
        let (status, retried) = harness
            .request_json(method.clone(), path, body.clone())
            .await?;
        assert_eq!(status, StatusCode::OK, "{retried}");
        for field in ["_id", "_seq_no", "_primary_term", "_version", "result"] {
            assert_eq!(retried[field], original[field], "{field}: {retried}");
        }
        record_event(
            output,
            "lost_response_retry",
            json!({"path": path, "receipt": retried}),
        )?;
    }
    assert_eq!(
        harness.local_sequence_states().await?,
        before,
        "retry allocated another operation"
    );
    Ok(())
}

#[tokio::test]
async fn lost_responses_survive_promotion_concurrent_mutations_and_rejoin() -> Result<()> {
    let mut harness = RestartClusterHarness::start().await?;
    let log_dir = harness.nodes[0]
        .config
        .log_path
        .parent()
        .context("node log has no directory")?;
    let mut output = fs::File::create(log_dir.join("d10-qualification.jsonl"))?;
    record_event(
        &mut output,
        "start",
        json!({
            "nodes": 3, "shards": 1, "replicas": 2, "minimum_durable_copies": 2,
            "durability": "request", "maximum_concurrent_client_mutations": RACING_DOCUMENTS * 2,
            "races": ["update/update", "delete/delete", "update/delete", "update/delete"],
            "faults": ["client_response_loss", "primary_process_kill", "old_primary_restart"],
            "excluded": ["power_loss", "unrestricted_divergence_rollback", "vector_snapshot_recovery"],
        }),
    )?;
    let index_uuid = harness.create_index_with_shards(1, 2).await?;
    let (status, settings) = harness
        .request_json(
            Method::PUT,
            &format!("/{INDEX_NAME}/_settings"),
            Some(json!({"index": {"minimum_durable_copies": 2}})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{settings}");
    assert_eq!(settings["acknowledged"], true, "{settings}");
    let (initial_state, initial_routing) = harness.wait_for_in_sync_replicas(2).await?;
    let old_primary_name = initial_routing.primary.clone();
    let old_allocation = allocation_id_for_node(&initial_state, INDEX_NAME, 0, &old_primary_name)?;
    let old_term = initial_state["indices"][INDEX_NAME]["shard_routing"]["0"]["primary_term"]
        .as_u64()
        .context("initial primary term is missing")?;
    record_event(&mut output, "initial_authority", initial_state)?;
    let mut oracle = BTreeMap::new();
    for (ordinal, id) in [
        "lost-update",
        "lost-delete",
        "delete-before-crash",
        "delete-while-offline",
    ]
    .into_iter()
    .map(str::to_string)
    .chain((0..RACING_DOCUMENTS).map(|document| format!("race-{document}")))
    .enumerate()
    {
        let source = expected_document(ordinal);
        let receipt = harness.put_document(&id, source.clone()).await?;
        oracle.insert(
            id,
            ExpectedDocument {
                source: Some(source),
                receipt,
            },
        );
    }
    let coordinator = harness
        .nodes
        .iter()
        .find(|node| node.config.name != old_primary_name)
        .context("non-primary coordinator is missing")?
        .config
        .clone();
    let mut lost = Vec::new();
    for (method, id, body) in [
        (
            Method::POST,
            "lost-update",
            Some(json!({"doc": {"n": 111, "payload": "lost-reply"}})),
        ),
        (Method::DELETE, "lost-delete", None),
    ] {
        let endpoint = if method == Method::POST {
            "_update"
        } else {
            "_doc"
        };
        let path = format!("/{INDEX_NAME}/{endpoint}/{id}?operation_id=d10-{id}");
        let receipt =
            discard_write_response(&harness, &coordinator, method.clone(), &path, body.clone())
                .await?;
        assert_eq!(receipt["_shards"]["successful"], 3, "{receipt}");
        let source = if body.is_some() {
            let mut source = oracle[id]
                .source
                .clone()
                .context("lost-update source is missing")?;
            source["n"] = json!(111);
            source["payload"] = json!("lost-reply");
            Some(source)
        } else {
            None
        };
        oracle.insert(
            id.into(),
            ExpectedDocument {
                source,
                receipt: receipt.clone(),
            },
        );
        record_event(
            &mut output,
            "response_discarded",
            json!({
                "id": id, "client_response_bytes": 0, "backend_receipt": receipt,
            }),
        )?;
        lost.push((method, path, body, receipt));
    }
    let deleted = harness.delete_document("delete-before-crash").await?;
    oracle.insert(
        "delete-before-crash".into(),
        ExpectedDocument {
            source: None,
            receipt: deleted,
        },
    );
    race_updates_and_deletes(&mut harness, 0, &mut oracle, &mut output).await?;
    assert_oracle(&mut harness, &oracle, &index_uuid).await?;
    record_event(
        &mut output,
        "before_primary_crash",
        serde_json::to_value(&oracle)?,
    )?;
    let old_primary = harness.stop_node(&old_primary_name)?;
    record_event(
        &mut output,
        "primary_killed",
        json!({"node": old_primary_name, "term": old_term}),
    )?;
    let promoted_routing = harness
        .wait_for_primary_promotion(&old_primary_name)
        .await?;
    assert!(
        initial_routing
            .in_sync_replicas
            .contains(&promoted_routing.primary)
    );
    let promoted = harness
        .wait_for_active_write_authority(old_term, Some(&old_primary_name))
        .await?;
    harness
        .wait_for_in_sync_replicas_with_unassigned(1, 1)
        .await?;
    record_event(&mut output, "promoted_authority", promoted)?;
    assert_oracle(&mut harness, &oracle, &index_uuid).await?;
    retry_lost_responses(&harness, &lost, &mut output).await?;
    race_updates_and_deletes(&mut harness, 1, &mut oracle, &mut output).await?;
    let deleted = harness.delete_document("delete-while-offline").await?;
    assert_eq!(deleted["_shards"]["successful"], 2, "{deleted}");
    oracle.insert(
        "delete-while-offline".into(),
        ExpectedDocument {
            source: None,
            receipt: deleted,
        },
    );
    let (status, updated) = harness
        .request_json(
            Method::POST,
            &format!("/{INDEX_NAME}/_update/lost-update"),
            Some(json!({"doc": {"n": 222, "payload": "new-primary"}})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert!(
        updated["_primary_term"]
            .as_u64()
            .is_some_and(|term| term > old_term),
        "{updated}"
    );
    let mut source = oracle["lost-update"]
        .source
        .clone()
        .context("updated source is missing")?;
    source["n"] = json!(222);
    source["payload"] = json!("new-primary");
    oracle.insert(
        "lost-update".into(),
        ExpectedDocument {
            source: Some(source),
            receipt: updated,
        },
    );
    assert_oracle(&mut harness, &oracle, &index_uuid).await?;
    record_event(
        &mut output,
        "before_old_primary_rejoin",
        serde_json::to_value(&oracle)?,
    )?;
    harness.restart_node(old_primary).await?;
    let (recovered, routing) = harness.wait_for_in_sync_replicas(2).await?;
    assert!(
        routing.in_sync_replicas.contains(&old_primary_name),
        "{routing:?}"
    );
    assert_ne!(
        allocation_id_for_node(&recovered, INDEX_NAME, 0, &old_primary_name)?,
        old_allocation,
        "old primary must recover under a fresh allocation"
    );
    for node in &harness.nodes {
        assert_copy_certificate(&node.config, &index_uuid, &recovered)?;
    }
    record_event(&mut output, "recovered_authority", recovered)?;
    retry_lost_responses(&harness, &lost, &mut output).await?;
    assert_oracle(&mut harness, &oracle, &index_uuid).await?;
    harness.flush_index_copies(3).await?;
    let replica = harness.stop_node(&old_primary_name)?;
    harness.restart_node(replica).await?;
    harness.wait_for_in_sync_replicas(2).await?;
    record_event(
        &mut output,
        "replica_restarted",
        json!({"node": old_primary_name}),
    )?;
    assert_oracle(&mut harness, &oracle, &index_uuid).await?;
    retry_lost_responses(&harness, &lost, &mut output).await?;
    assert_oracle(&mut harness, &oracle, &index_uuid).await?;
    let sequences = harness.local_sequence_states().await?;
    let max_sequence = sequences
        .values()
        .next()
        .context("no final copy sequence state")?
        .max_seq_no;
    for (node, sequence) in sequences {
        assert_eq!(sequence.max_seq_no, max_sequence, "{node}");
        assert_eq!(sequence.processed_checkpoint, max_sequence, "{node}");
        assert_eq!(sequence.persisted_checkpoint, max_sequence, "{node}");
        record_event(
            &mut output,
            "final_checkpoint",
            json!({
                "node": node, "max_seq_no": sequence.max_seq_no,
                "processed_checkpoint": sequence.processed_checkpoint,
                "persisted_checkpoint": sequence.persisted_checkpoint,
            }),
        )?;
    }
    harness.refresh_index().await?;
    harness
        .wait_for_count(
            oracle
                .values()
                .filter(|document| document.source.is_some())
                .count(),
        )
        .await?;
    record_event(&mut output, "complete", serde_json::to_value(&oracle)?)?;
    Ok(())
}
