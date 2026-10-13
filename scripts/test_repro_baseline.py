import unittest

from repro_baseline import (
    assert_equivalent,
    dataset_sha256,
    generate_documents,
    make_cases,
    markdown_report,
    metric_delta,
    normalize_search,
    parse_metrics,
    percentile,
    redact_storage_uri,
    sanitized_argv,
    summarize_latencies,
    validate_config,
)


class ReproBaselineTests(unittest.TestCase):
    def test_dataset_and_oracle_are_deterministic(self):
        documents = generate_documents(200, 42)
        self.assertEqual(documents, generate_documents(200, 42))
        self.assertNotEqual(documents, generate_documents(200, 43))
        self.assertEqual(dataset_sha256(documents), dataset_sha256(list(documents)))
        cases = {case["name"]: case for case in make_cases(documents)}
        self.assertEqual(cases["match_all"]["expected"]["matched"], 200)
        self.assertEqual(
            sum(cases["service_terms"]["expected"]["buckets"].values()), 200
        )

    def test_search_normalization_rejects_inexact_results(self):
        documents = generate_documents(200, 42)
        case = make_cases(documents)[0]
        response = {
            "_shards": {"failed": 0},
            "hits": {"total": {"value": 200, "relation": "eq"}},
        }
        assert_equivalent(normalize_search(response, case), case["expected"])
        response["hits"]["total"]["relation"] = "gte"
        with self.assertRaises(ValueError):
            normalize_search(response, case)
        with self.assertRaises(ValueError):
            normalize_search({"timed_out": True, "_shards": {"failed": 0}}, case)

    def test_metrics_parser_and_delta_preserve_labels(self):
        before = parse_metrics(
            "\n".join(
                [
                    'ferrissearch_object_store_requests_total{operation="get",outcome="success"} 2',
                    'ferrissearch_object_store_transfer_bytes_total{direction="download"} 128',
                    "ignored_metric 99",
                ]
            ),
            {
                "ferrissearch_object_store_requests_total",
                "ferrissearch_object_store_transfer_bytes_total",
            },
        )
        after = dict(before)
        for key in list(after):
            after[key] += 3
        self.assertEqual(set(metric_delta(before, after).values()), {3})

    def test_statistics_use_interpolated_percentiles(self):
        self.assertEqual(percentile([4, 1, 3, 2], 0.5), 2.5)
        self.assertAlmostEqual(percentile([4, 1, 3, 2], 0.95), 3.85)
        summary = summarize_latencies([1, 2, 3, 4])
        self.assertEqual(summary["samples"], 4)
        self.assertEqual(summary["p50_ms"], 2.5)

    def test_config_rejects_incomplete_matrix(self):
        config = {
            "profiles": ["local_shards", "remote_store"],
            "topology": {"nodes": 3},
            "dataset": {
                "documents": 100,
                "seed": 1,
                "bulk_batch_size": 10,
                "publish_batch_size": 10,
            },
            "execution": {
                "warmup_rounds": 1,
                "measured_rounds": 2,
                "concurrency": 1,
            },
        }
        validate_config(config)
        config["profiles"] = ["local_shards"]
        with self.assertRaises(ValueError):
            validate_config(config)

    def test_storage_uri_credentials_are_not_recorded(self):
        uri = "s3://access:secret@example.invalid:9443/prefix?token=signed"
        self.assertEqual(
            redact_storage_uri(uri), "s3://example.invalid:9443/prefix"
        )
        self.assertEqual(
            sanitized_argv(["runner", "--storage-uri", uri]),
            ["runner", "--storage-uri", "s3://example.invalid:9443/prefix"],
        )
        self.assertEqual(
            sanitized_argv(["runner", f"--storage-uri={uri}"]),
            ["runner", "--storage-uri=s3://example.invalid:9443/prefix"],
        )

    def test_markdown_is_generated_from_results(self):
        report = {
            "status": "completed",
            "started_at": "2026-10-13T00:00:00+00:00",
            "completed_at": "2026-10-13T00:01:00+00:00",
            "source": {"commit": "abc"},
            "build": {"profile": "release", "features": []},
            "topology": {"nodes": 3, "local_shards": 3, "replicas": 1},
            "dataset": {"documents": 100, "sha256": "deadbeef"},
            "execution": {
                "warmup_rounds": 1,
                "measured_rounds": 2,
                "concurrency": 1,
            },
            "object_store": {"uri_redacted": "file:///tmp/store"},
            "profiles": {
                "local_shards": {
                    "ingestion": {"documents_per_second": 1000},
                    "resources": {
                        "measured_metric_delta": {
                            "aggregate": {"process_cpu_seconds_total": 1.5}
                        },
                        "after_measured": {
                            "node-1": {
                                "metrics": {
                                    "process_resident_memory_bytes": 1024
                                },
                                "data_dir": {"allocated_bytes": 2048},
                            }
                        },
                        "object_store_profile_delta": {"aggregate": {}},
                    },
                    "measured": {
                        "requests_per_second": 500,
                        "cases": {
                            "match_all": {
                                "summary": {
                                    "p50_ms": 1,
                                    "p95_ms": 2,
                                    "p99_ms": 3,
                                }
                            }
                        },
                    },
                }
            },
        }
        markdown = markdown_report(report)
        self.assertIn("`local_shards`", markdown)
        self.assertIn("| `match_all` |", markdown)
        self.assertIn("correctness oracle", markdown)


if __name__ == "__main__":
    unittest.main()
