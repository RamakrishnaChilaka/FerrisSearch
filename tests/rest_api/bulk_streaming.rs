use super::*;

#[tokio::test]
async fn fs014_bulk_crosses_batch_boundaries_with_order_occ_retry_ids_and_one_final_visibility()
-> Result<()> {
    let harness = RestTestHarness::start().await?;
    create_write_contract_index(&harness, "fs014-order").await?;
    let (_, seed) = harness
        .put_json("/fs014-order/_doc/seed", json!({}))
        .await?;
    let term = seed["_primary_term"].as_u64().unwrap();
    let mut request = String::new();
    let mut expected = Vec::new();
    let mut append = |action: &'static str,
                      metadata: Value,
                      source: Option<Value>,
                      sequence: Option<u64>,
                      version: u64,
                      status: u16| {
        let id = metadata["_id"].as_str().unwrap().to_string();
        request.push_str(&json!({action: metadata}).to_string());
        request.push('\n');
        if let Some(source) = source {
            request.push_str(&source.to_string());
            request.push('\n');
        }
        expected.push((action, id, sequence, version, status));
    };
    for value in 0..300 {
        append(
            "index",
            json!({"_id":"x"}),
            Some(json!({"value":value})),
            Some(value + 1),
            value + 1,
            if value == 0 { 201 } else { 200 },
        );
    }
    append(
        "index",
        json!({"_id":"retry","_operation_id":"fs014-stable"}),
        Some(json!({"value":700})),
        Some(301),
        1,
        201,
    );
    for value in 0..300 {
        append(
            "index",
            json!({"_id":format!("f-{value}")}),
            Some(json!({"value":value})),
            Some(value + 302),
            1,
            201,
        );
    }
    append(
        "index",
        json!({"_id":"retry","_operation_id":"fs014-stable"}),
        Some(json!({"value":700})),
        Some(301),
        1,
        201,
    );
    append(
        "update",
        json!({"_id":"x"}),
        Some(json!({"doc":{"marker":902}})),
        Some(602),
        301,
        200,
    );
    append(
        "create",
        json!({"_id":"x"}),
        Some(json!({"wrong":true})),
        None,
        0,
        409,
    );
    append(
        "index",
        json!({"_id":"x","if_seq_no":602,"if_primary_term":term}),
        Some(json!({"value":900})),
        Some(603),
        302,
        200,
    );
    append(
        "index",
        json!({"_id":"x","if_seq_no":1,"if_primary_term":term}),
        Some(json!({"wrong":true})),
        None,
        0,
        409,
    );
    append("delete", json!({"_id":"x"}), None, Some(604), 303, 200);
    append(
        "create",
        json!({"_id":"x"}),
        Some(json!({"value":901})),
        Some(605),
        304,
        201,
    );
    append(
        "update",
        json!({"_id":"x"}),
        Some(json!({"doc":{"value":901}})),
        Some(605),
        304,
        200,
    );
    append("delete", json!({"_id":"absent"}), None, Some(606), 1, 404);
    append(
        "update",
        json!({"_id":"x"}),
        Some(json!({"doc":{"marker":902}})),
        Some(607),
        305,
        200,
    );
    let client = Client::builder().timeout(Duration::from_secs(60)).build()?;
    let response = client
        .post(format!(
            "{}/fs014-order/_bulk?refresh=true&pretty",
            harness.base_url
        ))
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(request)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await?;
    assert_eq!(response["errors"], true, "{response}");
    let items = response["items"].as_array().unwrap();
    assert_eq!(items.len(), expected.len());
    for (position, (action, id, sequence, version, status)) in expected.into_iter().enumerate() {
        let item = &items[position][action];
        assert_eq!(item["_id"], id, "item {position}: {item}");
        assert_eq!(item["status"], status, "item {position}: {item}");
        assert_eq!(
            item["_seq_no"].as_u64(),
            sequence,
            "item {position}: {item}"
        );
        if sequence.is_some() {
            assert_eq!(item["_primary_term"], term, "item {position}: {item}");
            assert_eq!(item["_version"], version, "item {position}: {item}");
            assert_eq!(item["forced_refresh"], true, "item {position}: {item}");
            assert!(item.get("_refresh").is_some(), "item {position}: {item}");
            let copies = if item["result"] == "noop" { 0 } else { 1 };
            assert_eq!(item["_shards"]["total"], copies, "item {position}: {item}");
            assert_eq!(
                item["_shards"]["successful"], copies,
                "item {position}: {item}"
            );
            assert_eq!(item["_shards"]["failed"], 0, "item {position}: {item}");
        } else {
            assert!(item.get("_primary_term").is_none(), "{item}");
            assert!(item.get("_refresh").is_none(), "{item}");
        }
    }
    let (_, document) = harness
        .get_json("/fs014-order/_doc/x?realtime=false")
        .await?;
    assert_eq!(document["_source"], json!({"value":901,"marker":902}));
    assert_eq!(document["_seq_no"], 607);
    assert_eq!(document["_version"], 305);
    assert_eq!(
        harness.get_json("/fs014-order/_count").await?.1["count"],
        303
    );
    Ok(())
}

#[tokio::test]
async fn fs014_pretty_bulk_response_larger_than_ten_mib_is_not_rebuffered() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let padding = "x".repeat(16 * 1024);
    let mut request = String::new();
    for position in 0..700 {
        request.push_str(
            &json!({"index":{"_index":"not-created","_id":format!("{position}-{padding}")}})
                .to_string(),
        );
        request.push_str("\n{broken\n");
    }
    let client = Client::builder().timeout(Duration::from_secs(60)).build()?;
    let response = client
        .post(format!("{}/_bulk?pretty", harness.base_url))
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(request)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.bytes().await?;
    assert!(bytes.len() > 10 * 1024 * 1024);
    assert!(bytes.contains(&b'\n'));
    let value: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(value["errors"], true);
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 700);
    for (position, item) in items.iter().enumerate() {
        assert_eq!(item["index"]["_id"], format!("{position}-{padding}"));
        assert_eq!(item["index"]["status"], 400);
        assert_eq!(item["index"]["error"]["type"], "mapper_parsing_exception");
    }
    assert!(
        harness
            .app_state
            .cluster_manager
            .get_state()
            .indices
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn fs014_oversized_source_keeps_its_item_and_later_valid_mutation() -> Result<()> {
    let harness = RestTestHarness::start().await?;
    let request = format!(
        "{{\"index\":{{\"_id\":\"oversized\"}}}}\n{{\"body\":\"{}\"}}\n\
         {{\"index\":{{\"_id\":\"valid\"}}}}\n{{\"body\":\"kept\"}}\n",
        "x".repeat(32 * 1024 * 1024)
    );
    let client = Client::builder().timeout(Duration::from_secs(60)).build()?;
    let response = client
        .post(format!("{}/fs014-lines/_bulk", harness.base_url))
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(request)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await?;
    assert_eq!(value["items"].as_array().unwrap().len(), 2, "{value}");
    assert_eq!(value["items"][0]["index"]["_id"], "oversized");
    assert_eq!(value["items"][0]["index"]["status"], 400);
    assert!(
        value["items"][0]["index"]["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("source line [2] exceeds")
    );
    assert_eq!(value["items"][1]["index"]["status"], 201);
    assert_eq!(value["items"][1]["index"]["_seq_no"], 0);
    assert_eq!(
        harness.get_json("/fs014-lines/_doc/oversized").await?.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        harness.get_json("/fs014-lines/_doc/valid").await?.1["_source"],
        json!({"body":"kept"})
    );
    Ok(())
}

#[tokio::test]
async fn fs014_post_mutation_spool_failure_is_indeterminate_and_operation_id_retry_is_exact()
-> Result<()> {
    let harness = RestTestHarness::start_internal_with_bulk(
        0,
        0,
        vec![NodeRole::Master, NodeRole::Data],
        None,
        ferrissearch::config::AppConfig {
            bulk_max_spool_bytes: 128,
            ..Default::default()
        },
    )
    .await?;
    create_write_contract_index(&harness, "fs014-spool").await?;
    let payload = json!({"value":17});
    let (status, body) = harness
        .post_ndjson(
            "/fs014-spool/_bulk",
            "{\"index\":{\"_id\":\"x\",\"_operation_id\":\"spool-op\"}}\n{\"value\":17}\n",
        )
        .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"]["type"], "write_outcome_unknown");
    let reason = body["error"]["reason"].as_str().unwrap();
    assert!(
        reason.contains("spool exceeds 128") && reason.contains("may have executed"),
        "{body}"
    );
    let (_, document) = harness.get_json("/fs014-spool/_doc/x").await?;
    assert_eq!(document["_source"], payload);
    assert_eq!(document["_seq_no"], 0);
    assert_eq!(document["_version"], 1);
    let (status, retry) = harness
        .put_json("/fs014-spool/_doc/x?operation_id=spool-op", payload)
        .await?;
    assert_eq!(status, StatusCode::CREATED, "{retry}");
    assert_eq!(retry["_seq_no"], 0);
    assert_eq!(retry["_version"], 1);
    Ok(())
}
