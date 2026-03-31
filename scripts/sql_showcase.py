#!/usr/bin/env python3
"""FerrisSearch SQL showcase — runs 18 queries against the hackernews index."""

import json
import sys
import time
import requests

BASE = "http://localhost:9200"

QUERIES = [
    ("Showcase: Rust authors by engagement",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up, avg(comments) AS avg_disc FROM "hackernews" WHERE text_match(title, \'rust\') GROUP BY author ORDER BY avg_up DESC LIMIT 10'),

    ("Complex: Database content creators (5 aggs + HAVING)",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up, min(upvotes) AS worst, max(upvotes) AS best, sum(comments) AS total_disc FROM "hackernews" WHERE text_match(title, \'database OR postgres OR redis OR sqlite\') AND comments > 5 GROUP BY author HAVING posts > 5 ORDER BY best DESC LIMIT 10'),

    ("AI/LLM creators",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up, max(comments) AS max_disc FROM "hackernews" WHERE text_match(title, \'GPT OR LLM OR ChatGPT\') GROUP BY author HAVING posts > 3 ORDER BY avg_up DESC LIMIT 10'),

    ("Startup founders",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up, sum(comments) AS total_disc, max(upvotes) AS best_post FROM "hackernews" WHERE text_match(title, \'startup OR founder\') AND upvotes > 10 GROUP BY author HAVING posts > 3 ORDER BY avg_up DESC LIMIT 10'),

    ("Security experts",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up, min(upvotes) AS worst, max(upvotes) AS best FROM "hackernews" WHERE text_match(title, \'security OR vulnerability OR breach\') AND comments > 10 GROUP BY author HAVING posts > 3 ORDER BY avg_up DESC LIMIT 10'),

    ("Show HN: Most discussed projects",
     'SELECT title, upvotes, comments, author FROM "hackernews" WHERE text_match(title, \'Show HN\') AND upvotes > 300 ORDER BY comments DESC LIMIT 10'),

    ("Ask HN: Most upvoted",
     'SELECT title, upvotes, comments, author FROM "hackernews" WHERE text_match(title, \'Ask HN\') AND upvotes > 500 ORDER BY upvotes DESC LIMIT 10'),

    ("Apple ecosystem engagement",
     'SELECT count(*) AS posts, avg(upvotes) AS avg_up, avg(comments) AS avg_disc FROM "hackernews" WHERE text_match(title, \'Apple OR iPhone OR macOS\')'),

    ("Google ecosystem engagement",
     'SELECT count(*) AS posts, avg(upvotes) AS avg_up, avg(comments) AS avg_disc FROM "hackernews" WHERE text_match(title, \'Google OR Android OR Chrome\')'),

    ("Cloud provider authors",
     'SELECT author, count(*) AS posts, sum(upvotes) AS total_up, avg(comments) AS avg_disc FROM "hackernews" WHERE text_match(title, \'AWS OR Azure OR cloud\') GROUP BY author HAVING posts > 5 ORDER BY total_up DESC LIMIT 10'),

    ("DevOps/container experts",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up FROM "hackernews" WHERE text_match(title, \'Kubernetes OR Docker OR container\') GROUP BY author HAVING posts > 3 ORDER BY avg_up DESC LIMIT 10'),

    ("Layoff discussions",
     'SELECT title, upvotes, comments, author FROM "hackernews" WHERE text_match(title, \'layoff OR fired\') AND comments > 100 ORDER BY comments DESC LIMIT 10'),

    ("Salary posts",
     'SELECT title, upvotes, comments FROM "hackernews" WHERE text_match(title, \'salary OR compensation\') AND upvotes > 100 ORDER BY upvotes DESC LIMIT 10'),

    ("Elon/Tesla/SpaceX",
     'SELECT count(*) AS posts, avg(upvotes) AS avg_up, max(comments) AS top_disc FROM "hackernews" WHERE text_match(title, \'Elon OR Tesla OR SpaceX\')'),

    ("Open source engagement",
     'SELECT count(*) AS posts, avg(upvotes) AS avg_up, avg(comments) AS avg_disc, max(upvotes) AS best FROM "hackernews" WHERE text_match(title, \'open source\')'),

    ("Hiring trends",
     'SELECT author, count(*) AS posts, sum(upvotes) AS total_up FROM "hackernews" WHERE text_match(title, \'hiring OR jobs\') AND comments > 20 GROUP BY author HAVING posts > 5 ORDER BY total_up DESC LIMIT 10'),

    ("Rust vs Go (EXPLAIN)",
     'SELECT author, count(*) AS posts, avg(upvotes) AS avg_up FROM "hackernews" WHERE text_match(title, \'rust OR golang\') GROUP BY author HAVING posts > 5 ORDER BY avg_up DESC LIMIT 10'),

    ("Crypto engagement",
     'SELECT count(*) AS posts, avg(upvotes) AS avg_up FROM "hackernews" WHERE text_match(title, \'blockchain OR crypto OR bitcoin\')'),
]


def run_query(label, sql):
    start = time.time()
    try:
        resp = requests.post(
            f"{BASE}/hackernews/_sql/explain",
            json={"query": sql, "analyze": True},
            timeout=30,
        )
        elapsed_wall = (time.time() - start) * 1000
        data = resp.json()

        if resp.status_code != 200:
            error = data.get("error", {}).get("reason", resp.text[:100])
            return label, None, elapsed_wall, f"HTTP {resp.status_code}: {error}"

        mode = data.get("execution_strategy", data.get("execution_mode", "?"))
        matched = data.get("matched_hits", 0)
        rows = data.get("rows", [])
        timings = data.get("timings", {})
        total_ms = timings.get("total_ms", elapsed_wall)
        search_ms = timings.get("search_ms", 0)
        merge_ms = timings.get("merge_ms", 0)
        shards = data.get("_shards", {})

        return label, {
            "mode": mode,
            "matched": matched,
            "rows": len(rows),
            "total_ms": total_ms,
            "search_ms": search_ms,
            "merge_ms": merge_ms,
            "shards": shards,
            "top_rows": rows[:3],
        }, elapsed_wall, None

    except Exception as e:
        elapsed_wall = (time.time() - start) * 1000
        return label, None, elapsed_wall, str(e)


def main():
    # Check cluster health
    try:
        health = requests.get(f"{BASE}/_cluster/health", timeout=5).json()
        indices = requests.get(f"{BASE}/_cat/indices", timeout=5).text.strip()
        print(f"Cluster: {health.get('cluster_name')} | Status: {health.get('status')} | Nodes: {health.get('number_of_nodes')}")
        print(f"Indices: {indices}")
    except Exception as e:
        print(f"Cannot connect to {BASE}: {e}")
        sys.exit(1)

    print(f"\nRunning {len(QUERIES)} queries...\n")
    print(f"{'#':>2}  {'Query':<45} {'Mode':<28} {'Matched':>8} {'Rows':>5} {'Time':>8}  {'Wall':>8}")
    print("─" * 120)

    total_server = 0
    total_wall = 0
    errors = 0

    for i, (label, sql) in enumerate(QUERIES, 1):
        label_str, result, wall_ms, error = run_query(label, sql)

        if error:
            print(f"{i:>2}  {label_str:<45} {'ERROR':<28} {'':>8} {'':>5} {'':>8}  {wall_ms:>7.1f}ms")
            print(f"    └─ {error}")
            errors += 1
            total_wall += wall_ms
            continue

        r = result
        total_server += r["total_ms"]
        total_wall += wall_ms

        print(f"{i:>2}  {label_str:<45} {r['mode']:<28} {r['matched']:>8,} {r['rows']:>5} {r['total_ms']:>7.1f}ms  {wall_ms:>7.1f}ms")

        # Print top rows for grouped queries
        if r["top_rows"] and any(k in str(r["top_rows"][0]) for k in ["author", "title"]):
            for row in r["top_rows"][:2]:
                cols = []
                for k, v in row.items():
                    if isinstance(v, float):
                        cols.append(f"{k}={v:.0f}")
                    else:
                        cols.append(f"{k}={v}")
                print(f"    └─ {', '.join(cols)}")

    print("─" * 120)
    print(f"\n{len(QUERIES)} queries | {errors} errors | Server total: {total_server:.1f}ms | Wall total: {total_wall:.1f}ms")
    print(f"Avg server time: {total_server / len(QUERIES):.1f}ms | Avg wall time: {total_wall / len(QUERIES):.1f}ms")


if __name__ == "__main__":
    main()
