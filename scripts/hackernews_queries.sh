#!/usr/bin/env bash
# Hacker News SQL benchmark queries for FerrisSearch
# Run against a cluster with HN stories in the hackernews index
#
# Usage:
#   bash scripts/hackernews_queries.sh
#   bash scripts/hackernews_queries.sh http://localhost:9201  # target a specific node

HOST="${1:-http://localhost:9200}"

sql() {
  local name="$1"
  local query="$2"
  echo ""
  echo "=== $name ==="
  local start=$(date +%s%N)
  curl -s "$HOST/hackernews/_sql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\"}" | python3 -m json.tool
  local end=$(date +%s%N)
  local ms=$(( (end - start) / 1000000 ))
  echo "--- ${ms}ms ---"
}

dsl() {
  local name="$1"
  local body="$2"
  echo ""
  echo "=== $name ==="
  local start=$(date +%s%N)
  curl -s -XPOST "$HOST/hackernews/_search" \
    -H 'Content-Type: application/json' \
    -d "$body" | python3 -m json.tool
  local end=$(date +%s%N)
  local ms=$(( (end - start) / 1000000 ))
  echo "--- ${ms}ms ---"
}

count_api() {
  local name="$1"
  local body="$2"
  echo ""
  echo "=== $name ==="
  local start=$(date +%s%N)
  if [ -z "$body" ]; then
    curl -s "$HOST/hackernews/_count" | python3 -m json.tool
  else
    curl -s -XPOST "$HOST/hackernews/_count" \
      -H 'Content-Type: application/json' \
      -d "$body" | python3 -m json.tool
  fi
  local end=$(date +%s%N)
  local ms=$(( (end - start) / 1000000 ))
  echo "--- ${ms}ms ---"
}

explain() {
  local name="$1"
  local query="$2"
  echo ""
  echo "=== $name (EXPLAIN ANALYZE) ==="
  curl -s "$HOST/hackernews/_sql/explain" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\", \"analyze\": true}" | python3 -m json.tool
}

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  FerrisSearch — Hacker News SQL Benchmark                      ║"
echo "║  Demonstrates hybrid text_match + structured SQL analytics     ║"
echo "╚══════════════════════════════════════════════════════════════════╝"

# --- count(*) fast path ---

count_api "Q1: Total story count (_count API, match_all fast path)" ""

sql "Q2: Total story count (SQL count_star_fast)" \
  "SELECT count(*) AS total_stories FROM \\\"hackernews\\\""

# --- _count with query ---

count_api "Q3: How many posts mention 'rust'? (_count + text match)" \
  '{"query": {"match": {"title": "rust"}}}'

count_api "Q4: How many posts mention 'machine learning'?" \
  '{"query": {"match": {"title": "machine learning"}}}'

# --- Grouped partials (text_match + GROUP BY) ---

sql "Q5: Top authors posting about 'machine learning'" \
  "SELECT author, count(*) AS posts, avg(comments) AS avg_comments FROM \\\"hackernews\\\" WHERE text_match(title, 'machine learning') GROUP BY author ORDER BY posts DESC"

sql "Q6: Top authors posting about 'rust'" \
  "SELECT author, count(*) AS posts, avg(comments) AS avg_comments FROM \\\"hackernews\\\" WHERE text_match(title, 'rust') GROUP BY author ORDER BY posts DESC"

sql "Q7: Global stats — avg upvotes and comments across all stories" \
  "SELECT count(*) AS total, avg(comments) AS avg_comments, max(comments) AS max_comments, avg(upvotes) AS avg_upvotes, max(upvotes) AS max_upvotes FROM \\\"hackernews\\\""

# --- Fast-field projection (text_match + ORDER BY LIMIT) ---

sql "Q8: Top 'startup' posts by comment count" \
  "SELECT title, comments, author FROM \\\"hackernews\\\" WHERE text_match(title, 'startup') AND comments > 100 ORDER BY comments DESC LIMIT 10"

sql "Q9: Top 'AI' posts by comment count" \
  "SELECT title, comments, author FROM \\\"hackernews\\\" WHERE text_match(title, 'artificial intelligence') ORDER BY comments DESC LIMIT 10"

sql "Q10: Top 'blockchain' posts by comment count" \
  "SELECT title, comments, author FROM \\\"hackernews\\\" WHERE text_match(title, 'blockchain crypto') ORDER BY comments DESC LIMIT 10"

# --- Predicate pushdown + fast fields ---

sql "Q11: Stories with 500+ comments (range pushdown)" \
  "SELECT title, comments, upvotes, author FROM \\\"hackernews\\\" WHERE comments > 500 ORDER BY comments DESC LIMIT 10"

sql "Q12: Stories with comments BETWEEN 200 AND 400" \
  "SELECT title, comments, upvotes, author FROM \\\"hackernews\\\" WHERE comments BETWEEN 200 AND 400 ORDER BY comments DESC LIMIT 10"

# --- DSL search (non-SQL) ---

dsl "Q13: DSL text search for 'rust' sorted by upvotes" \
  '{"query": {"match": {"title": "rust"}}, "sort": [{"upvotes": "desc"}], "size": 5}'

dsl "Q14: DSL bool query — 'python' but NOT 'java'" \
  '{"query": {"bool": {"must": [{"match": {"title": "python"}}], "must_not": [{"match": {"title": "java"}}]}}, "size": 5}'

dsl "Q15: DSL aggregation — top authors by post count" \
  '{"query": {"match_all": {}}, "size": 0, "aggs": {"top_authors": {"terms": {"field": "author", "size": 10}}}}'

dsl "Q16: DSL aggregation — comment stats" \
  '{"query": {"match_all": {}}, "size": 0, "aggs": {"comment_stats": {"stats": {"field": "comments"}}}}'

# --- EXPLAIN ANALYZE ---

explain "Q17: EXPLAIN ANALYZE — text_match + GROUP BY (grouped partials)" \
  "SELECT author, count(*) AS posts, avg(comments) AS avg_comments FROM \\\"hackernews\\\" WHERE text_match(title, 'python') GROUP BY author ORDER BY posts DESC"

explain "Q18: EXPLAIN ANALYZE — text_match + filter + ORDER BY LIMIT (fast fields)" \
  "SELECT title, comments, author FROM \\\"hackernews\\\" WHERE text_match(title, 'startup') AND comments > 100 ORDER BY comments DESC LIMIT 10"

echo ""
echo "Done. All queries executed against $HOST"
