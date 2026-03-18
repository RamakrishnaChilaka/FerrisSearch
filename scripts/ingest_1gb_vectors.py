#!/usr/bin/env python3
"""
Ingest ~1 GB of synthetic vector data into FerrisSearch via opensearch-py.

Usage:
    # With venv (recommended):
    /tmp/osenv/bin/python3 scripts/ingest_1gb_vectors.py

    # Custom host/port/dimensions:
    /tmp/osenv/bin/python3 scripts/ingest_1gb_vectors.py --host localhost --port 9200 --dims 128

Each document has:
  - title (text), category (keyword): for hybrid search testing
  - embedding (knn_vector, 128-dim by default): random normalized vector

At 128 dims (~700 bytes/doc), ~1.5M docs ≈ 1 GB.
"""

import argparse
import math
import random
import time

from opensearchpy import OpenSearch, helpers

INDEX_NAME = "benchmark-vectors"
BATCH_SIZE = 2000  # smaller batches — vector payloads are larger

CATEGORIES = ["science", "tech", "art", "sports", "finance", "health", "education", "travel"]
WORDS = [
    "neural", "network", "transformer", "attention", "embedding", "vector",
    "similarity", "cosine", "distance", "nearest", "neighbor", "search",
    "index", "cluster", "shard", "distributed", "parallel", "inference",
    "model", "training", "feature", "dimension", "latent", "space",
    "retrieval", "ranking", "relevance", "semantic", "hybrid", "fusion",
]


def random_unit_vector(dims: int) -> list:
    """Generate a random unit vector (L2 norm = 1.0)."""
    raw = [random.gauss(0, 1) for _ in range(dims)]
    norm = math.sqrt(sum(x * x for x in raw))
    if norm < 1e-10:
        raw[0] = 1.0
        norm = 1.0
    return [round(x / norm, 6) for x in raw]


def estimate_doc_bytes(dims: int) -> int:
    """Rough estimate of JSON bytes per document."""
    # Vector: dims * ~9 chars per float (e.g. "0.123456,") + brackets
    vec_bytes = dims * 9 + 2
    # Text fields + metadata: ~150 bytes
    return vec_bytes + 150


def generate_doc(doc_id: int, dims: int) -> dict:
    return {
        "_index": INDEX_NAME,
        "_id": str(doc_id),
        "_source": {
            "title": f"Item {doc_id} {random.choice(WORDS)} {random.choice(WORDS)}",
            "category": random.choice(CATEGORIES),
            "embedding": random_unit_vector(dims),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest ~1 GB of vector data into FerrisSearch")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--target-gb", type=float, default=1.0)
    parser.add_argument("--dims", type=int, default=128, help="Vector dimensions")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    doc_bytes = estimate_doc_bytes(args.dims)
    target_bytes = int(args.target_gb * 1_000_000_000)
    total_docs = target_bytes // doc_bytes

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        use_ssl=False,
        verify_certs=False,
        timeout=120,
        max_retries=3,
        retry_on_timeout=True,
    )

    info = client.info()
    print(f"Connected to {info['name']} v{info['version']}")
    print(f"Vector dims: {args.dims}, ~{doc_bytes} bytes/doc, ~{total_docs:,} total docs")

    if client.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists, deleting...")
        client.indices.delete(index=INDEX_NAME)

    print(f"Creating index '{INDEX_NAME}' with knn_vector mapping...")
    client.indices.create(
        index=INDEX_NAME,
        body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "category": {"type": "keyword"},
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": args.dims,
                    },
                }
            },
        },
    )

    print(f"Ingesting ~{total_docs:,} docs ({args.target_gb} GB) in batches of {args.batch_size}...")
    start = time.time()
    ingested = 0
    errors_total = 0

    batch = []
    for i in range(total_docs):
        batch.append(generate_doc(i, args.dims))
        if len(batch) >= args.batch_size:
            try:
                success, errs = helpers.bulk(client, batch, raise_on_error=False)
                ingested += success
                if isinstance(errs, list):
                    errors_total += len(errs)
                else:
                    errors_total += errs
            except Exception as e:
                print(f"\nBulk error at doc {ingested}: {e}")
                errors_total += len(batch)

            elapsed = time.time() - start
            rate = ingested / elapsed if elapsed > 0 else 0
            mb_sent = (ingested * doc_bytes) / (1024 * 1024)
            pct = (ingested / total_docs) * 100
            print(
                f"\r  {ingested:>10,} / {total_docs:,} docs "
                f"({pct:5.1f}%) | {mb_sent:,.0f} MB | "
                f"{rate:,.0f} docs/s | errors: {errors_total}",
                end="",
                flush=True,
            )
            batch = []

    # Final batch
    if batch:
        try:
            success, errs = helpers.bulk(client, batch, raise_on_error=False)
            ingested += success
            if isinstance(errs, list):
                errors_total += len(errs)
            else:
                errors_total += errs
        except Exception as e:
            print(f"\nFinal bulk error: {e}")
            errors_total += len(batch)

    elapsed = time.time() - start
    rate = ingested / elapsed if elapsed > 0 else 0
    mb_total = (ingested * doc_bytes) / (1024 * 1024)

    print(f"\n\n{'=' * 60}")
    print(f"Vector ingestion complete!")
    print(f"  Documents:  {ingested:,} indexed, {errors_total:,} errors")
    print(f"  Dimensions: {args.dims}")
    print(f"  Data:       ~{mb_total:,.0f} MB")
    print(f"  Time:       {elapsed:.1f}s")
    print(f"  Rate:       {rate:,.0f} docs/s")
    print(f"{'=' * 60}")

    # Refresh and verify
    print("\nRefreshing index...")
    try:
        client.indices.refresh(index=INDEX_NAME)
    except Exception:
        pass

    # Verify text search works
    try:
        resp = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 0})
        print(f"Text search: {resp['hits']['total']['value']:,} docs in index")
    except Exception as e:
        print(f"Text search verify error: {e}")

    # Verify kNN search works
    print("\nTesting kNN search (k=5)...")
    query_vec = random_unit_vector(args.dims)
    try:
        resp = client.search(
            index=INDEX_NAME,
            body={
                "knn": {
                    "embedding": {
                        "vector": query_vec,
                        "k": 5,
                    }
                }
            },
        )
        hits = resp["hits"]["hits"]
        print(f"kNN returned {len(hits)} hits:")
        for h in hits:
            print(f"  _id={h['_id']}  _score={h['_score']:.4f}  title={h['_source'].get('title', '?')}")
    except Exception as e:
        print(f"kNN search error: {e}")


if __name__ == "__main__":
    main()
