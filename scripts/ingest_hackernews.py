#!/usr/bin/env python3
"""Ingest Hacker News stories from Parquet into FerrisSearch.

Usage:
    /tmp/ferris-venv/bin/python3 scripts/ingest_hackernews.py
    /tmp/ferris-venv/bin/python3 scripts/ingest_hackernews.py --max-docs 500000
    /tmp/ferris-venv/bin/python3 scripts/ingest_hackernews.py --host localhost --port 9200
"""

import argparse
import json
import time
import requests
import pyarrow.parquet as pq

PARQUET_FILE = "test_workload_data/hacker_story.parquet"
INDEX_NAME = "hackernews"
BATCH_SIZE = 5000


def create_index(host: str, port: int):
    url = f"http://{host}:{port}/{INDEX_NAME}"
    body = {
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 0,
            "refresh_interval_ms": 30000,
        },
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "url": {"type": "keyword"},
                "author": {"type": "keyword"},
                "upvotes": {"type": "integer"},
                "comments": {"type": "integer"},
                "time": {"type": "integer"},
            }
        },
    }

    # Delete if exists
    requests.delete(url)
    resp = requests.put(url, json=body)
    if resp.status_code != 200:
        print(f"Failed to create index: {resp.status_code} {resp.text}")
        return False
    print(f"Created index '{INDEX_NAME}' with 3 shards")
    return True


def ingest(host: str, port: int, max_docs: int | None):
    table = pq.read_table(PARQUET_FILE)
    total_rows = len(table)
    target = min(total_rows, max_docs) if max_docs else total_rows
    print(f"Parquet: {total_rows:,} rows, ingesting {target:,}")

    bulk_url = f"http://{host}:{port}/{INDEX_NAME}/_bulk"
    ingested = 0
    errors = 0
    start = time.time()
    batch_times = []

    for batch_start in range(0, target, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, target)
        batch = table.slice(batch_start, batch_end - batch_start)

        ndjson_lines = []
        for i in range(len(batch)):
            row = {col: batch.column(col)[i].as_py() for col in batch.column_names}

            doc_id = str(row.get("id", batch_start + i))
            title = row.get("title") or ""
            url = row.get("url") or ""
            author = row.get("author") or ""
            upvotes = row.get("score") or 0
            comments = row.get("comments") or 0
            ts = row.get("time") or 0

            action = json.dumps({"index": {"_id": doc_id}})
            doc = json.dumps(
                {
                    "title": title,
                    "url": url,
                    "author": author,
                    "upvotes": upvotes,
                    "comments": comments,
                    "time": ts,
                }
            )
            ndjson_lines.append(action)
            ndjson_lines.append(doc)

        body = "\n".join(ndjson_lines) + "\n"

        batch_start_t = time.time()
        try:
            resp = requests.post(
                bulk_url,
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=120,
            )
            if resp.status_code != 200:
                errors += batch_end - batch_start
                print(f"  Batch error: {resp.status_code}")
            else:
                ingested += batch_end - batch_start
        except Exception as e:
            errors += batch_end - batch_start
            print(f"  Batch exception: {e}")

        batch_ms = (time.time() - batch_start_t) * 1000
        batch_times.append(batch_ms)

        if len(batch_times) % 20 == 0:
            elapsed = time.time() - start
            rate = ingested / elapsed if elapsed > 0 else 0
            print(
                f"  {ingested:>8,} / {target:,}  "
                f"({ingested * 100 / target:.1f}%)  "
                f"{rate:.0f} docs/sec  "
                f"last batch {batch_ms:.0f}ms"
            )

    elapsed = time.time() - start
    rate = ingested / elapsed if elapsed > 0 else 0
    print(f"\nDone: {ingested:,} ingested, {errors:,} errors, {elapsed:.1f}s, {rate:.0f} docs/sec")

    # Refresh to make searchable
    print("Refreshing index...")
    requests.post(f"http://{host}:{port}/{INDEX_NAME}/_refresh")
    print("Ready for queries!")


def main():
    parser = argparse.ArgumentParser(description="Ingest HN stories into FerrisSearch")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--max-docs", type=int, default=None, help="Limit docs to ingest")
    args = parser.parse_args()

    if not create_index(args.host, args.port):
        return

    ingest(args.host, args.port, args.max_docs)


if __name__ == "__main__":
    main()
