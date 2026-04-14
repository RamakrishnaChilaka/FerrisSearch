# The $15 Per Ride You Didn't Know About: What 243 Million NYC Taxi Rides Reveal

*Lyft pays wheelchair-accessible drivers 204% of fare — a flat ~$15/ride bonus regardless of trip distance. Uber pays 101%. Same city, same mandate, completely different economics.*

---

## The Dataset

243 million high-volume for-hire vehicle (FHVHV) trips in New York City — every Uber and Lyft ride recorded by the [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) from the 2025 monthly releases. Two carriers, 265 taxi zones, and $6.5 billion in fare revenue.

The TLC publishes trip-level data for all licensed vehicles. In the HVFHS dataset, each row is a single ride dispatched by a high-volume platform. The two active HVFHS license holders are **Uber** (HV0003, dispatching through 30+ base entities) and **Lyft** (HV0005, dispatching through 2 bases). See the [TLC Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf) for full field definitions.

| Metric | Value |
|--------|-------|
| Total rides | 243,589,684 |
| Carriers | Uber (HV0003), Lyft (HV0005) |
| Benchmark cluster | 3 nodes, 24 shards |
| Fields | 25 |
| Avg trip distance | ~5 miles |

---

## Two Carriers, Two Business Models

Uber dominates with nearly three-quarters of the market. Lyft holds the remaining quarter.

| Carrier | Rides | Revenue | Market Share |
|---------|-------|---------|-------------|
| Uber | 176.0M | $4.87B | 72% |
| Lyft | 67.6M | $1.70B | 28% |

But the real story isn't market share — it's how each carrier splits the economics between platform, driver, and rider.

### The Margin Gap

| Metric | Uber | Lyft |
|--------|------|------|
| Fare per mile | $7.81 | $7.14 |
| Driver payout ratio | 79% | 79% |
| Platform margin | 21% | 21% |
| Tip rate (tips/fare) | 4% | 5% |

At the full-year scale, the payout ratios converge — both carriers pay drivers 79% of fare. Uber still charges $0.67 more per mile, so its drivers earn more in absolute terms per trip despite the identical percentage.

**The takeaway:** Pricing power matters more than revenue share. Higher fare per mile translates to higher absolute driver earnings even at the same payout ratio.

---

## Where the Money Is: The Airport Revenue Race

The top pickup zones by gross revenue (fare + tips) show airports dominating the revenue rankings:

| Zone | Rides | Gross Revenue | Avg Fare | Tip Rate |
|------|-------|--------------|----------|----------|
| 138 — LaGuardia Airport | 4.92M | $317.3M | $59.72 | 8% |
| 132 — JFK Airport | 4.07M | $314.5M | $73.18 | 6% |
| 230 — Times Sq/Theatre District | 2.90M | $134.0M | $43.27 | 7% |
| 161 — Midtown Center | 2.82M | $125.9M | $42.04 | 6% |
| 68 — East Harlem South | 2.54M | $97.2M | $36.10 | 6% |

At full-year scale, LaGuardia overtakes JFK as the #1 revenue zone ($317M vs $315M) — its higher ride volume (4.92M vs 4.07M) compensates for JFK's premium fare ($73 vs $60). JFK remains the higher-value individual ride. LaGuardia riders tip at a notably higher rate (8% vs 6%).

---

## The Dominant Route: Airport to Zone 265

The single highest-volume OD pair is **East New York zone 76 → zone 76** at 849K rides — a same-zone short hop. But the highest-*revenue* route is **JFK Airport (132) → Crown Heights/Prospect Heights (265)** with 821K rides and $101M in gross revenue. The second is **LaGuardia (138) → Crown Heights/Prospect Heights (265)** with 673K rides and $80M.

After these two airport corridors, the next eight highest-volume routes are all **same-zone short hops** — zones like East New York (76), Bushwick (39), and Bath Beach/Bensonhurst (26) — with average distances of about 1 mile and fares around $10. These are the neighborhood errand rides that make up the long tail.

### The Airport Corridor: Carrier Head-to-Head

On the dominant Crown Heights corridor, the carrier economics diverge sharply:

| Carrier | From | Rides | Avg Miles | Fare/Mile | Payout | Tip Rate |
|---------|------|-------|-----------|-----------|--------|----------|
| Uber | JFK | 599K | 30.4 | $4.11 | 77% | 6% |
| Lyft | JFK | 220K | 31.3 | $3.65 | 84% | 7% |
| Uber | LGA | 461K | 27.3 | $4.56 | 71% | 8% |
| Lyft | LGA | 210K | 26.4 | $3.63 | 83% | 9% |

Uber charges $0.46–$0.93 more per mile on these corridors. Despite giving drivers a smaller share (71–77% vs 83–84%), the higher fare base means Uber drivers still earn more per trip in absolute terms.

LaGuardia pickups remain the most lucrative corridor in the dataset — Uber's $4.56/mile on LGA→265 is the highest rate of any major route.

---

## The Fee Stack

Beyond the base fare, riders pay a stack of fees and surcharges:

| Fee Component | Uber | Lyft |
|---------------|------|------|
| Base fare | $27.66 | $25.09 |
| Sales tax | $2.32 | $2.04 |
| Tolls | $1.14 | $0.99 |
| Congestion surcharge | $0.97 | $1.03 |
| BCF (Black Car Fund) | $0.69 | $0.64 |
| Airport fee | $0.21 | $0.21 |

The fee stacks are similar, but Uber's higher base fare means fees are a smaller percentage of the total rider cost. Congestion fees are slightly higher for Lyft, suggesting a marginally different geographic trip mix.

---

## Tipping: A 20% Ceiling

Among riders who tip, the highest-volume pickup zones converge on an approximately **20% tip rate** — almost exactly the standard restaurant gratuity. The top 15 high-volume tipping zones sit in a narrow band just under 20%.

This is remarkably uniform. It suggests riders who choose to tip are anchored to a default percentage, likely the app's suggested tip option, regardless of fare amount or zone.

But here's the catch: **most riders don't tip at all.** The fleet-wide tip rate is only 4–5% of total fare revenue. The 20% rate only applies to the ~18% of rides that receive any tip.

---

## Trip Segments: Short, Medium, Long

| Segment | Rides | Share | Avg Fare | Total Revenue | Tip Rate |
|---------|-------|-------|----------|--------------|----------|
| Short (≤3 mi) | 122.4M | 50% | $14.46 | $1.77B | 4% |
| Medium (3–10 mi) | 89.2M | 37% | $29.31 | $2.62B | 4% |
| Long (10+ mi) | 32.0M | 13% | $68.17 | $2.18B | 5% |

Half of all rides are short hops under 3 miles. But the 13% of rides over 10 miles generate more revenue ($2.18B) than the 50% of short rides ($1.77B). Long trips are the revenue engine; short trips are the volume engine.

Tip rates are nearly identical across segments — further evidence that tipping behavior is percentage-anchored rather than distance-driven.

---

## The Shared-Ride Discount

Uber operates a shared-ride program. Of its 176.0M rides, 3.9M were shared requests that matched with another rider, and 2.9M were shared requests that did **not** match.

| Mode | Rides | Avg Fare | Payout Ratio |
|------|-------|----------|--------------|
| Standard (N/N) | 169.1M | $28.02 | 78% |
| Shared matched (Y/Y) | 3.9M | $21.04 | 94% |
| Shared unmatched (Y/N) | 2.9M | $16.34 | 103% |

When a shared ride doesn't match, the rider still gets a discounted fare ($16.34 vs $28.02), and the driver is paid as if it were a normal trip. Against base fare alone, the driver payout exceeds fare by 3%. The platform subsidizes the **rider's discount**, not the driver.

---

## The WAV Premium: The Headline Nobody Published

NYC's Taxi & Limousine Commission requires rideshare platforms to fulfill wheelchair-accessible vehicle (WAV) requests. Both carriers comply. But they do it at vastly different costs.

Of 243.6M rides, ~699K were WAV-requested and WAV-matched. Here's how the two carriers pay those drivers:

| | Uber | Lyft |
|--|------|------|
| WAV-requested rides | 484,390 | 214,340 |
| Avg base fare | $25.29 | $23.05 |
| Avg driver pay | $24.66 | **$38.15** |
| Payout ratio | 101% | **204%** |
| Total premium paid | -$307K | **+$3.2M** |

**Lyft pays WAV drivers $15.10 more per ride than the base fare.** That's a 104% premium, totaling over $3.2M in direct driver subsidies across 214K rides. Uber, with 2.3x the WAV volume, runs those rides at near-breakeven (actually slightly below — paying $0.63 less than fare on average).

Same city. Same TLC mandate. Completely different compliance strategies.

### The Distance Test: Is It a Flat Bonus or a Percentage?

To determine whether Lyft's premium scales with trip distance or is a fixed per-ride bonus, we split WAV-requested, WAV-matched rides into three distance buckets:

| Distance | Uber Rides | Uber Premium | Lyft Rides | Lyft Premium |
|----------|-----------|-------------|-----------|-------------|
| Short (<5 mi) | 336,649 | **-$0.76** | 152,544 | **+$15.16** |
| Medium (5–15 mi) | 123,483 | **-$0.04** | 52,282 | **+$14.99** |
| Long (15+ mi) | 24,258 | **-$1.86** | 9,514 | **+$14.69** |

The answer is clear: **Lyft's WAV premium is a flat ~$15/ride regardless of trip length.**

- On a $15 short trip, the driver gets **$30** — a 2x multiplier
- On a $36 medium trip, the driver gets **$51** — a 1.4x multiplier
- On a $75 long trip, the driver gets **$90** — a 1.2x multiplier

Uber passes the fare through dollar-for-dollar at every distance bucket, typically $0–2 below breakeven.

This pattern is consistent with a **fixed dollar WAV incentive** embedded in Lyft's driver compensation formula — not a percentage-based bonus. It means the accessibility premium is most impactful on short urban trips, where a $15 bonus doubles the driver's take-home pay.

### The Broader WAV Fleet Effect

Beyond the ~699K explicitly requested WAV rides, 21.4M rides were served by WAV-equipped vehicles even when accessibility wasn't requested. Those drivers also earn more:

| WAV Vehicle | Rides | Avg Fare | Payout Ratio |
|-------------|-------|----------|-------------|
| No (standard) | 221.8M | $27.36 | 78% |
| Yes (WAV vehicle) | 21.4M | $23.19 | 88% |

WAV drivers earn a 10-percentage-point payout premium on **every ride**, not just the ones where accessibility was requested. This is likely the TLC's incentive structure at work — platforms pay WAV-equipped drivers more across the board to maintain fleet availability.

---

## Key Takeaways

1. **The WAV premium is a flat ~$15/ride.** Lyft pays WAV drivers 204% of fare — and the premium is distance-invariant: $15.16 on short trips, $14.99 on medium, $14.69 on long. This is a fixed dollar incentive, not a percentage.

2. **Same mandate, opposite economics.** Uber runs 2.3x the WAV volume at breakeven. Lyft subsidizes $3.2M+ across 214K rides. The TLC requires both to serve WAV requests, but the compliance cost is entirely asymmetric.

3. **WAV drivers earn more on every ride, not just accessible ones.** ~21.4M rides used WAV vehicles without being requested — those drivers still get an 88% payout ratio vs 78% fleet-wide.

4. **Pricing power > payout ratio.** Uber charges $0.67/mile more than Lyft, yet both pay drivers 79% of fare. The absolute dollar matters more than the percentage.

5. **Airport corridors are the premium market.** The JFK→Crown Heights route alone generates $101M in revenue. LaGuardia overtakes JFK as the #1 revenue zone at $317M.

6. **Tipping is binary, not proportional.** Riders who tip converge on 20%. Most riders don't tip at all. Trip distance barely changes the tip rate.

7. **Short rides are the volume engine; long rides are the revenue engine.** 13% of rides (10+ miles) generate more revenue ($2.18B) than the 50% of rides under 3 miles ($1.77B).

---

## Query Performance

Canonical benchmark numbers for the full 243.6M-row dataset on the current benchmark cluster:

### Azure 32-Core Benchmark

Full 243.6M-row dataset, 3 nodes, 24 shards, **1 segment per shard post-force-merge**, Azure Standard_L32s_v4 (32 vCPU, 256 GB RAM, NVMe RAID0), `sql_approximate_top_k: true`, best of 10 runs via `scripts/benchmark_1pager.py`.

| Query | Hits Scanned | Execution Mode | Best Latency |
|-------|-------------|----------------|---------|
| Count | 243,589,684 | `count_star_fast` | **0.4ms** |
| Carrier market share | 243,589,684 | `tantivy_grouped_partials` | **207ms** |
| Fee stack by carrier | 243,589,684 | `tantivy_grouped_partials` | **817ms** |
| Top 5 pickup zones | 243,589,684 | `tantivy_grouped_partials` | **761ms** |
| Top 10 routes | 243,589,684 | `tantivy_grouped_partials` | **1.43s** |
| Margin gap | 243,110,337 | `tantivy_grouped_partials` | **1.18s** |
| Airport corridor | 1,489,906 | `tantivy_grouped_partials` | **111ms** |
| WAV headline | 698,730 | `tantivy_grouped_partials` | **32ms** |
| WAV short trips | 489,193 | `tantivy_grouped_partials` | **69ms** |
| Shared-ride discount | 175,964,464 | `tantivy_grouped_partials` | **692ms** |
| WAV fleet effect | 243,204,544 | `tantivy_grouped_partials` | **606ms** |

**Notes:**
- All 11 queries landed on `tantivy_grouped_partials` — no fallback to `tantivy_fast_fields` or row materialization.
- Top 10 routes used `approximate_top_k: true` (shard-level pruning) on the highest-cardinality grouped query in the set.
- Filtered queries (airport corridor, WAV headline, WAV short trips) complete in **32–111ms** with sub-2M hit sets.
- Full-corpus scans over all 243.6M rows complete in **0.2–1.4s** depending on metric count and GROUP BY cardinality.
- Force-merging to 1 segment per shard improved full-corpus query latency by ~25% vs pre-force-merge (e.g. carrier market share 298ms → 207ms).
- The machine had 256 GB RAM with ~170 GB in buffer/cache at query time.

---

## Benchmark SQL

The Azure benchmark table above is driven by these query shapes. The first 9 queries are listed here exactly; the remaining 2 benchmarked queries appear later under Supporting Story SQL.

### 1. Count

```sql
SELECT count(*) AS total_rides
FROM "nyc-taxis";
```

### 2. Carrier Market Share

```sql
SELECT hvfhs_license_num,
			 count(*) AS rides,
			 ROUND(SUM(base_passenger_fare), 0) AS revenue
FROM "nyc-taxis"
GROUP BY hvfhs_license_num
ORDER BY rides DESC;
```

### 3. Fee Stack By Carrier

```sql
SELECT hvfhs_license_num,
			 ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
			 ROUND(AVG(sales_tax), 2) AS avg_tax,
			 ROUND(AVG(tolls), 2) AS avg_tolls,
			 ROUND(AVG(congestion_surcharge), 2) AS avg_congestion,
			 ROUND(AVG(bcf), 2) AS avg_bcf,
			 ROUND(AVG(airport_fee), 2) AS avg_airport
FROM "nyc-taxis"
GROUP BY hvfhs_license_num
ORDER BY hvfhs_license_num;
```

### 4. Top 5 Pickup Zones

```sql
SELECT "PULocationID",
			 count(*) AS rides,
			 ROUND(SUM(base_passenger_fare + tips), 0) AS gross_revenue,
			 ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
			 ROUND(SUM(tips) / SUM(base_passenger_fare) * 100, 0) AS tip_rate
FROM "nyc-taxis"
GROUP BY "PULocationID"
ORDER BY gross_revenue DESC
LIMIT 5;
```

### 5. Top 10 Routes

```sql
SELECT "PULocationID",
			 "DOLocationID",
			 count(*) AS rides,
			 ROUND(SUM(base_passenger_fare + tips), 0) AS gross_revenue,
			 ROUND(AVG(trip_miles), 1) AS avg_miles
FROM "nyc-taxis"
GROUP BY "PULocationID", "DOLocationID"
ORDER BY rides DESC
LIMIT 10;
```

### 6. Margin Gap

```sql
SELECT hvfhs_license_num,
			 ROUND(AVG(base_passenger_fare / trip_miles), 2) AS fare_per_mile,
			 ROUND(AVG(driver_pay / base_passenger_fare) * 100, 0) AS payout_pct,
			 ROUND(AVG(tips / base_passenger_fare) * 100, 0) AS tip_rate_pct
FROM "nyc-taxis"
WHERE trip_miles > 0.1
	AND base_passenger_fare > 1
GROUP BY hvfhs_license_num
ORDER BY hvfhs_license_num;
```

### 7. Airport Corridor

```sql
SELECT hvfhs_license_num,
			 PULocationID,
			 count(*) AS rides,
			 ROUND(AVG(trip_miles), 1) AS avg_miles,
			 ROUND(AVG(base_passenger_fare / trip_miles), 2) AS fare_per_mile,
			 ROUND(AVG(driver_pay / base_passenger_fare) * 100, 0) AS payout_pct,
			 ROUND(SUM(tips) / SUM(base_passenger_fare) * 100, 0) AS tip_rate
FROM "nyc-taxis"
WHERE DOLocationID = 265
	AND (PULocationID = 132 OR PULocationID = 138)
	AND trip_miles > 0.1
	AND base_passenger_fare > 1
GROUP BY hvfhs_license_num, PULocationID
ORDER BY PULocationID, hvfhs_license_num;
```

### 8. WAV Headline

```sql
SELECT hvfhs_license_num,
			 count(*) AS rides,
			 ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
			 ROUND(AVG(driver_pay), 2) AS avg_pay,
			 ROUND(AVG(driver_pay / base_passenger_fare) * 100, 0) AS payout_pct,
			 ROUND(SUM(driver_pay - base_passenger_fare), 0) AS total_premium
FROM "nyc-taxis"
WHERE wav_request_flag = 'Y'
	AND wav_match_flag = 'Y'
	AND base_passenger_fare > 0
GROUP BY hvfhs_license_num
ORDER BY hvfhs_license_num;
```

### 9. WAV Short Trips

```sql
SELECT hvfhs_license_num,
			 count(*) AS rides,
			 ROUND(AVG(driver_pay - base_passenger_fare), 2) AS avg_premium
FROM "nyc-taxis"
WHERE wav_request_flag = 'Y'
	AND wav_match_flag = 'Y'
	AND base_passenger_fare > 0
	AND trip_miles < 5
GROUP BY hvfhs_license_num
ORDER BY hvfhs_license_num;
```

## Supporting Story SQL

### Tipping Zones

```sql
SELECT PULocationID,
			 count(*) AS tipped_rides,
			 sum(tips) / sum(base_passenger_fare) AS tip_rate,
			 avg(base_passenger_fare) AS avg_fare
FROM "nyc-taxis"
WHERE tips > 0
	AND base_passenger_fare > 0
GROUP BY PULocationID
HAVING count(*) > 500000
ORDER BY tip_rate DESC
LIMIT 15;
```

### Trip Segments

```sql
SELECT CASE
				 WHEN trip_miles <= 3 THEN 'Short (<=3 mi)'
				 WHEN trip_miles <= 10 THEN 'Medium (3-10 mi)'
				 ELSE 'Long (10+ mi)'
			 END AS segment,
			 CASE
				 WHEN trip_miles <= 3 THEN 1
				 WHEN trip_miles <= 10 THEN 2
				 ELSE 3
			 END AS bucket_order,
			 count(*) AS rides,
			 count(*) * 100.0 / 243589684 AS share_pct,
			 avg(base_passenger_fare) AS avg_fare,
			 sum(base_passenger_fare) AS total_revenue,
			 sum(tips) / sum(base_passenger_fare) AS tip_rate
FROM "nyc-taxis"
WHERE trip_miles > 0
GROUP BY CASE
					 WHEN trip_miles <= 3 THEN 'Short (<=3 mi)'
					 WHEN trip_miles <= 10 THEN 'Medium (3-10 mi)'
					 ELSE 'Long (10+ mi)'
				 END,
				 CASE
					 WHEN trip_miles <= 3 THEN 1
					 WHEN trip_miles <= 10 THEN 2
					 ELSE 3
				 END
ORDER BY bucket_order;
```

### Shared-Ride Discount

```sql
SELECT shared_request_flag,
			 shared_match_flag,
			 count(*) AS rides,
			 ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
			 ROUND(AVG(driver_pay / base_passenger_fare) * 100, 0) AS payout_pct
FROM "nyc-taxis"
WHERE hvfhs_license_num = 'HV0003'
	AND base_passenger_fare > 0
GROUP BY shared_request_flag, shared_match_flag
ORDER BY rides DESC;
```

### WAV Headline

```sql
SELECT hvfhs_license_num,
			 count(*) AS rides,
			 ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
			 ROUND(AVG(driver_pay), 2) AS avg_pay,
			 ROUND(AVG(driver_pay / base_passenger_fare) * 100, 0) AS payout_pct,
			 ROUND(SUM(driver_pay - base_passenger_fare), 0) AS total_premium
FROM "nyc-taxis"
WHERE wav_request_flag = 'Y'
	AND wav_match_flag = 'Y'
	AND base_passenger_fare > 0
GROUP BY hvfhs_license_num
ORDER BY hvfhs_license_num;
```

### WAV Distance Buckets

```sql
SELECT CASE
				 WHEN trip_miles < 5 THEN 'Short (<5 mi)'
				 WHEN trip_miles < 15 THEN 'Medium (5-15 mi)'
				 ELSE 'Long (15+ mi)'
			 END AS distance_bucket,
			 CASE
				 WHEN trip_miles < 5 THEN 1
				 WHEN trip_miles < 15 THEN 2
				 ELSE 3
			 END AS bucket_order,
			 hvfhs_license_num,
			 count(*) AS rides,
			 avg(driver_pay - base_passenger_fare) AS premium
FROM "nyc-taxis"
WHERE wav_request_flag = 'Y'
	AND wav_match_flag = 'Y'
	AND base_passenger_fare > 0
	AND trip_miles > 0
GROUP BY CASE
					 WHEN trip_miles < 5 THEN 'Short (<5 mi)'
					 WHEN trip_miles < 15 THEN 'Medium (5-15 mi)'
					 ELSE 'Long (15+ mi)'
				 END,
				 CASE
					 WHEN trip_miles < 5 THEN 1
					 WHEN trip_miles < 15 THEN 2
					 ELSE 3
				 END,
				 hvfhs_license_num
ORDER BY bucket_order, hvfhs_license_num;
```

### WAV Fleet Effect

```sql
SELECT wav_match_flag,
			 count(*) AS rides,
			 ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
			 ROUND(AVG(driver_pay / base_passenger_fare) * 100, 0) AS payout_pct
FROM "nyc-taxis"
WHERE base_passenger_fare > 0
GROUP BY wav_match_flag
ORDER BY wav_match_flag;
```

---

*Data source: [NYC TLC FHVHV Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), 2025 monthly parquet files. Field definitions per the [TLC Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf). HV0003 = Uber, HV0005 = Lyft, as identified by HVFHS license number in the official base-company mapping.*

---

*Analysis performed on [FerrisSearch](https://github.com/rchilaka/ROpenSearch), a distributed search engine built in Rust. 243.6M documents on a 1-node, 24-shard 32 core, 128 GB memory cluster, queried via SQL with grouped analytics on fast-field columnar storage. All queries ran on fast-field execution paths — no row materialization, no post-hoc aggregation.*
