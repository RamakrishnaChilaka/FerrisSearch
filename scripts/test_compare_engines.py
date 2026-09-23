import math
import unittest

from compare_engines import (
    FERRIS_PROFILE, assert_equivalent, generate_documents, make_cases, normalize,
    oracle, percentile, validate_index_settings, validate_runtime_profile,
)


class ComparisonTests(unittest.TestCase):
    def test_generation_is_reproducible_and_has_unique_ids(self):
        documents = generate_documents(2000, 20260921)
        self.assertEqual(documents, generate_documents(2000, 20260921))
        self.assertNotEqual(documents, generate_documents(2000, 1))
        self.assertEqual(len({doc["event_id"] for doc in documents}), 2000)
        for case in make_cases("ferris-compare-test"):
            self.assertGreater(oracle(documents, case)["matched"], 0)

    def test_grouped_oracle_uses_all_matching_values(self):
        documents = [
            {"service": "checkout", "duration_ms": 10},
            {"service": "checkout", "duration_ms": 30},
            {"service": "payments", "duration_ms": 5},
        ]
        result = oracle(documents, {"name": "group", "kind": "grouped", "selection": "all"})
        self.assertEqual(result, {
            "matched": 3,
            "rows": [
                {"service": "checkout", "events": 2, "avg_duration": 20, "max_duration": 30},
                {"service": "payments", "events": 1, "avg_duration": 5, "max_duration": 5},
            ],
        })

    def test_hits_oracle_checks_values_and_order(self):
        documents = generate_documents(2000, 20260921)
        case = make_cases("ferris-compare-test")[0]
        expected = oracle(documents, case)
        response = {
            "_shards": {"failed": 0},
            "hits": {
                "total": {"value": expected["matched"], "relation": "eq"},
                "hits": [{"_source": row} for row in expected["rows"]],
            },
        }
        assert_equivalent(normalize(response, case, "opensearch"), expected)
        response["hits"]["hits"].reverse()
        with self.assertRaises(ValueError):
            assert_equivalent(normalize(response, case, "opensearch"), expected)

    def test_inexact_or_failed_responses_are_rejected(self):
        case = make_cases("ferris-compare-test")[0]
        with self.assertRaises(ValueError):
            normalize({"_shards": {"failed": 1}}, case, "opensearch")
        with self.assertRaises(ValueError):
            normalize({"timed_out": True}, case, "opensearch")
        with self.assertRaises(ValueError):
            normalize({
                "_shards": {"failed": 0},
                "hits": {"total": {"value": 10000, "relation": "gte"}},
            }, case, "opensearch")
        grouped = make_cases("ferris-compare-test")[-1]
        with self.assertRaises(ValueError):
            normalize({
                "_shards": {"failed": 0}, "approximate_top_k": True,
            }, grouped, "ferris")

    def test_numeric_comparison_and_percentiles(self):
        assert_equivalent({"count": 3.0, "mean": 1.0 + 1e-12}, {"count": 3, "mean": 1.0})
        for value in (math.nan, math.inf, 1.01):
            with self.assertRaises(ValueError):
                assert_equivalent(value, 1.0)
        self.assertEqual(percentile([4, 1, 3, 2], 0.5), 2.5)
        self.assertAlmostEqual(percentile([4, 1, 3, 2], 0.95), 3.85)

    def test_runtime_profile_rejects_unmatched_resources(self):
        node = {
            "jvm": {"mem": {"heap_max_in_bytes": 1073741824}},
            "settings": {"node": {"processors": "4"}},
        }
        affinities = {"ferris": {0, 1, 2, 3}, "opensearch": {0, 1, 2, 3}}
        result = validate_runtime_profile(affinities, {4, 5}, node, dict(FERRIS_PROFILE))
        self.assertEqual(result["opensearch_heap_bytes"], 1073741824)
        with self.assertRaises(ValueError):
            validate_runtime_profile(affinities, {0, 4}, node, dict(FERRIS_PROFILE))
        with self.assertRaises(ValueError):
            validate_runtime_profile({"ferris": {0}, "opensearch": {1}}, {4}, node, dict(FERRIS_PROFILE))
        with self.assertRaises(ValueError):
            validate_runtime_profile(affinities, {4}, node, {})
        node["jvm"]["mem"]["heap_max_in_bytes"] = 536870912
        with self.assertRaises(ValueError):
            validate_runtime_profile(affinities, {4}, node, dict(FERRIS_PROFILE))

    def test_index_profile_rejects_async_durability(self):
        body = {"events": {"settings": {"index": {
            "number_of_shards": "1", "number_of_replicas": "0",
            "refresh_interval": "600s", "translog": {"durability": "request"},
            "requests": {"cache": {"enable": "false"}},
        }}}}
        validate_index_settings(body, "events", "opensearch")
        body["events"]["settings"]["index"]["translog"]["durability"] = "async"
        with self.assertRaises(ValueError):
            validate_index_settings(body, "events", "opensearch")


if __name__ == "__main__":
    unittest.main()
