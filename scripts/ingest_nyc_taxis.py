#!/usr/bin/env python3
"""
Fast parallel ingest of NYC FHVHV taxi trip data from Parquet into FerrisSearch.

Uses multiple worker threads and columnar batch conversion for high throughput.

Usage:
    /tmp/ferris-venv/bin/python3 scripts/ingest_nyc_taxis.py
    /tmp/ferris-venv/bin/python3 scripts/ingest_nyc_taxis.py --workers 8 --batch-size 10000
    /tmp/ferris-venv/bin/python3 scripts/ingest_nyc_taxis.py --max-rows 5000000
"""

import argparse
import queue
import statistics
import threading
import time

import pyarrow.parquet as pq
from opensearchpy import OpenSearch, helpers

INDEX_NAME = "nyc-taxis"

MAPPINGS = {
    "properties": {
        "hvfhs_license_num": {"type": "keyword"},
        "dispatching_base_num": {"type": "keyword"},
        "originating_base_num": {"type": "keyword"},
        "request_datetime": {"type": "keyword"},
        "on_scene_datetime": {"type": "keyword"},
        "pickup_datetime": {"type": "keyword"},
        "dropoff_datetime": {"type": "keyword"},
        "PULocationID": {"type": "integer"},
        "DOLocationID": {"type": "integer"},
        "trip_miles": {"type": "float"},
        "trip_time": {"type": "integer"},
        "base_passenger_fare": {"type": "float"},
        "tolls": {"type": "float"},
        "bcf": {"type": "float"},
        "sales_tax": {"type": "float"},
        "congestion_surcharge": {"type": "float"},
        "airport_fee": {"type": "float"},
        "tips": {"type": "float"},
        "driver_pay": {"type": "float"},
        "shared_request_flag": {"type": "keyword"},
        "shared_match_flag": {"type": "keyword"},
        "access_a_ride_flag": {"type": "keyword"},
        "wav_request_flag": {"type": "keyword"},
        "wav_match_flag": {"type": "keyword"},
        "cbd_congestion_fee": {"type": "float"},
    }
}

TIMESTAMP_COLS = {
    "request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime"
}


def batch_to_bulk_actions(table, start_id):
    columns = {}
    for name in table.column_names:
        columns[name] = table.column(name).to_pylist()

    num_rows = table.num_rows
    actions = []
    for i in range(num_rows):
        doc = {}
        for name, col_data in columns.items():
            val = col_data[i]
            if val is None:
                continue
            if name in TIMESTAMP_COLS and hasattr(val, "isoformat"):
                doc[name] = val.isoformat()
            else:
                doc[name] = val
        actions.append({
            "_index": INDEX_NAME,
            "_id": str(start_id + i),
            "_source": doc,
        })
    return actions


class IngestStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ingested = 0
        self.errors = 0
        self.batch_latencies = []

    def add(self, success, errors, latency_ms):
        with self.lock:
            self.ingested += success
            self.errors += errors
            self.batch_latencies.append(latency_ms)

    def snapshot(self):
        with self.lock:
            return self.ingested, self.errors


def worker_fn(work_queue, host, port, stats):
    client = OpenSearch(
        hosts=[{"host": host, "port": port}],
        use_ssl=False,
        verify_certs=False,
        timeout=120,
        max_retries=3,
        retry_on_timeout=True,
    )
    while True:
        item = work_queue.get()
        if item is None:
            work_queue.task_done()
            break
        actions = item
        start = time.perf_counter()
        try:
            success, errs = helpers.bulk(client, actions, raise_on_error=False)
            ms = (time.perf_counter() - start) * 1000
            err_count = len(errs) if isinstance(errs, list) else errs
            stats.add(success, err_count, ms)
        except Exception:
            ms = (time.perf_counter() - start) * 1000
            stats.add(0, len(actions), ms)
        work_queue.task_done()


def main():
    parser = argparse.ArgumentParser(description="Fast parallel ingest of NYC taxi Parquet")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--parquet", default="test_workload_data/fhvhv_tripdata_2026-01.parquet")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--shards", type=int, default=3)
    parser.add_argument("--parquet-batch", type=int, default=50000)
    args = parser.parse_args()

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        use_ssl=False, verify_certs=False, timeout=120,
    )

    info = client.info()
    print(f"Connected to {info['name']} v{info['version']}")

    pf = pq.ParquetFile(args.parquet)
    total_rows = pf.metadata.num_rows
    if args.max_rows:
        total_rows = min(total_rows, args.max_rows)
    print(f"Parquet: {pf.metadata.num_rows:,} total rows, ingesting {total_rows:,}")
    print(f"Config: {args.workers} workers, batch_size={args.batch_size}, parquet_batch={args.parquet_batch}")

    if client.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists, deleting...")
        client.indices.delete(index=INDEX_NAME)

    print(f"Creating index '{INDEX_NAME}' with {args.shards} shards...")
    client.indices.create(
        index=INDEX_NAME,
        body={
            "settings": {"number_of_shards": args.shards, "number_of_replicas": 0},
            "mappings": MAPPINGS,
        },
    )

    stats = IngestStats()
    work_queue = queue.Queue(maxsize=args.workers * 2)

    threads = []
    for _ in range(args.workers):
        t = threading.Thread(target=worker_fn, args=(work_queue, args.host, args.port, stats), daemon=True)
        t.start()
        threads.append(t)

    start = time.time()
    doc_id = 0
    produced = 0

    for batch_table in pf.iter_batches(batch_size=args.parquet_batch):
        if args.max_rows and produced >= args.max_rows:
            break

        remaining = total_rows - produced
        take = min(batch_table.num_rows, remaining)
        if take < batch_table.num_rows:
            batch_table = batch_table.slice(0, take)

        actions = batch_to_bulk_actions(batch_table, doc_id)
        doc_id += len(actions)
        produced += len(actions)

        for i in range(0, len(actions), args.batch_size):
            chunk = actions[i:i + args.batch_size]
            work_queue.put(chunk)

        ingested, errors = stats.snapshot()
        elapsed = time.time() - start
        rate = ingested / elapsed if elapsed > 0 else 0
        pct = (ingested / total_rows) * 100
        print(
            f"\r  {ingested:>12,} / {total_rows:,} docs "
            f"({pct:5.1f}%) | {rate:,.0f} docs/s | errors: {errors} | q: {work_queue.qsize()}",
            end="", flush=True,
        )

    work_queue.join()

    for _ in threads:
        work_queue.put(None)
    for t in threads:
        t.join(timeout=10)

    ingested, errors = stats.snapshot()
    elapsed = time.time() - start
    rate = ingested / elapsed if elapsed > 0 else 0

    print(f"\n\n{'=' * 60}")
    print(f"NYC Taxi ingestion complete!")
    print(f"  Documents: {ingested:,} indexed, {errors:,} errors")
    print(f"  Workers:   {args.workers}")
    print(f"  Time:      {elapsed:.1f}s")
    print(f"  Rate:      {rate:,.0f} docs/s")

    with stats.lock:
        lats = sorted(stats.batch_latencies)
    if lats:
        num = len(lats)
        def pct(p):
            k = (num - 1) * (p / 100)
            f = int(k)
            c = min(f + 1, num - 1)
            return lats[f] + (k - f) * (lats[c] - lats[f])
        print(f"\n  Bulk batch latency ({num:,} batches of {args.batch_size} docs):")
        print(f"    Min:  {lats[0]:>8.1f} ms")
        print(f"    Avg:  {statistics.mean(lats):>8.1f} ms")
        print(f"    p50:  {pct(50):>8.1f} ms")
        print(f"    p95:  {pct(95):>8.1f} ms")
        print(f"    p99:  {pct(99):>8.1f} ms")
        print(f"    Max:  {lats[-1]:>8.1f} ms")
    print(f"{'=' * 60}")

    print("\nRefreshing index...")
    try:
        client.indices.refresh(index=INDEX_NAME)
    except Exception:
        pass
    try:
        resp = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 0})
        print(f"Verified: {resp['hits']['total']['value']:,} docs in index")
    except Exception as e:
        print(f"Verify error: {e}")


if __name__ == "__main__":
    main()
