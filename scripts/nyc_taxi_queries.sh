#!/usr/bin/env bash
# NYC Taxi SQL benchmark queries for FerrisSearch
# Run against a cluster with 1M+ docs in the nyc-taxis index
#
# Usage:
#   bash scripts/nyc_taxi_queries.sh
#   bash scripts/nyc_taxi_queries.sh http://localhost:9201  # target a specific node

HOST="${1:-http://localhost:9200}"

sql() {
  local name="$1"
  local query="$2"
  echo ""
  echo "=== $name ==="
  local start=$(date +%s%N)
  curl -s "$HOST/nyc-taxis/_sql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\"}" | python3 -m json.tool
  local end=$(date +%s%N)
  local ms=$(( (end - start) / 1000000 ))
  echo "--- ${ms}ms ---"
}

explain() {
  local name="$1"
  local query="$2"
  echo ""
  echo "=== $name (EXPLAIN ANALYZE) ==="
  curl -s "$HOST/nyc-taxis/_sql/explain" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\", \"analyze\": true}" | python3 -m json.tool
}

# --- Aggregation queries (grouped partials path) ---

sql "Q1: Total trip count" \
  "SELECT count(*) AS total_trips FROM \\\"nyc-taxis\\\""

sql "Q2: Average fare by license type" \
  "SELECT hvfhs_license_num, avg(base_passenger_fare) AS avg_fare, count(*) AS trips FROM \\\"nyc-taxis\\\" GROUP BY hvfhs_license_num ORDER BY trips DESC"

sql "Q3: Average distance, fare, and tips by license" \
  "SELECT hvfhs_license_num, avg(trip_miles) AS avg_miles, avg(base_passenger_fare) AS avg_fare, avg(tips) AS avg_tips FROM \\\"nyc-taxis\\\" GROUP BY hvfhs_license_num"

sql "Q4: Driver pay stats by license" \
  "SELECT hvfhs_license_num, avg(driver_pay) AS avg_pay, sum(driver_pay) AS total_pay, count(*) AS trips FROM \\\"nyc-taxis\\\" GROUP BY hvfhs_license_num ORDER BY avg_pay DESC"

sql "Q7: Fare distribution (min/max/avg/sum)" \
  "SELECT min(base_passenger_fare) AS min_fare, max(base_passenger_fare) AS max_fare, avg(base_passenger_fare) AS avg_fare, sum(base_passenger_fare) AS total_fare FROM \\\"nyc-taxis\\\""

# --- Predicate pushdown queries ---

sql "Q5: Long trips (BETWEEN pushdown)" \
  "SELECT hvfhs_license_num, trip_miles, base_passenger_fare FROM \\\"nyc-taxis\\\" WHERE trip_miles BETWEEN 20 AND 50 ORDER BY trip_miles DESC LIMIT 10"

sql "Q6: License filter (IN pushdown)" \
  "SELECT hvfhs_license_num, avg(base_passenger_fare) AS avg_fare, avg(tips) AS avg_tips, count(*) AS trips FROM \\\"nyc-taxis\\\" WHERE hvfhs_license_num IN ('HV0003', 'HV0005') GROUP BY hvfhs_license_num ORDER BY trips DESC"

# --- Fast-field projection queries ---

sql "Q9: Top 10 highest fares" \
  "SELECT hvfhs_license_num, base_passenger_fare, trip_miles, tips FROM \\\"nyc-taxis\\\" ORDER BY base_passenger_fare DESC LIMIT 10"

sql "Q10: Top 10 longest trips" \
  "SELECT hvfhs_license_num, trip_miles, trip_time, base_passenger_fare FROM \\\"nyc-taxis\\\" ORDER BY trip_miles DESC LIMIT 10"

sql "Q11: Trips with high tips" \
  "SELECT hvfhs_license_num, tips, base_passenger_fare, trip_miles FROM \\\"nyc-taxis\\\" WHERE tips > 50 ORDER BY tips DESC LIMIT 10"

sql "Q12: Short expensive trips" \
  "SELECT hvfhs_license_num, trip_miles, base_passenger_fare FROM \\\"nyc-taxis\\\" WHERE trip_miles BETWEEN 0.5 AND 2.0 AND base_passenger_fare > 50 ORDER BY base_passenger_fare DESC LIMIT 10"

# --- EXPLAIN ANALYZE ---

explain "Q8: EXPLAIN ANALYZE grouped partials" \
  "SELECT hvfhs_license_num, avg(base_passenger_fare) AS avg_fare, count(*) AS trips FROM \\\"nyc-taxis\\\" GROUP BY hvfhs_license_num ORDER BY trips DESC"

explain "Q13: EXPLAIN ANALYZE fast-field with BETWEEN" \
  "SELECT hvfhs_license_num, trip_miles, base_passenger_fare FROM \\\"nyc-taxis\\\" WHERE trip_miles BETWEEN 20 AND 50 ORDER BY trip_miles DESC LIMIT 10"

echo ""
echo "Done. All queries executed against $HOST"
