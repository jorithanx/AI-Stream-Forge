# Feature Consumption Examples

Runnable reference implementations for reading StreamForge user-event features
in two access patterns: **offline batch** (training, analytics) and **online
low-latency** (model serving, real-time decisioning).

---

## Physical Layout

Two separate layouts exist in MinIO/S3, each optimised for a different access
pattern.

### Offline — Iceberg on MinIO (partition-pruned Parquet)

```
s3a://streamforge/warehouse/
  streamforge/
    user_event_counts/
      metadata/
        v1.metadata.json
        snap-<id>.avro
      data/
        event_date=2024-01-13/
          00000-0-<uuid>-00001.parquet   ← ~128 MB columnar file
        event_date=2024-01-14/
          00000-0-<uuid>-00001.parquet
        event_date=2024-01-15/
          00000-0-<uuid>-00001.parquet
          00000-0-<uuid>-00002.parquet   ← second file when partition grows
```

- **Partition field**: `event_date` (identity transform, `YYYY-MM-DD` UTC)
- **File format**: Parquet (columnar, ~4× compression over JSON)
- **Query engine**: PyIceberg → Apache Arrow → pandas DataFrame
- **Partition pruning**: only partitions within the requested date range are opened

### Online — MinIO Serving Layout + Redis Cache

```
s3://processed/
  features/
    serving/
      latest/
        us/                          ← 2-char alphanumeric shard prefix
          user-smith.json
          user-stubbs.json
        jo/
          john-doe.json
        an/
          anonymous-99.json
        sy/
          synthetic-user-0001.json
```

Each object is a single JSON document — the user's **most recent** feature snapshot:

```json
{
  "user_id":         "user-smith",
  "event_count":     42,
  "event_rate":      0.7,
  "window_start_ms": 1705276800000,
  "window_end_ms":   1705276860000,
  "event_date":      "2024-01-15",
  "materialized_at": "2024-01-15T02:05:33Z"
}
```

Redis sits in front of MinIO as an L1 cache:

```
Redis key   : feature:v1:{user_id}
Redis value : same JSON string as MinIO object
Redis TTL   : REDIS_TTL_S (default 3600 s for online reads,
                            86400 s for materialization warm-up)
```

---

## Read Paths

### Offline: BatchFeatureReader

```
read_date_range("2024-01-13", "2024-01-15")
        │
        ▼
  Backend selection (auto)
        │
        ├─► Iceberg (PyIceberg)
        │      table.scan(row_filter="event_date >= '2024-01-13'
        │                          AND event_date <= '2024-01-15'")
        │      .to_arrow() → pandas DataFrame
        │      (reads only the 3 relevant Parquet partitions)
        │
        ├─► MinIO raw JSON (fallback when Iceberg unavailable)
        │      list_objects("streamforge/features/2024/01/13/") +
        │      list_objects("streamforge/features/2024/01/14/") +
        │      list_objects("streamforge/features/2024/01/15/")
        │      → download + parse each .json file
        │
        └─► Synthetic (fallback when no services available)
               deterministic RNG — 50 users × 24 windows/day
               zero network I/O, useful for CI and local dev
```

### Online: OnlineFeatureReader

```
get("user-smith")
        │
        ▼
  L1: Redis GET feature:v1:user-smith
        │
        ├─ hit  → return FeatureVector (<2 ms)
        │
        └─ miss
               │
               ▼
         L2: MinIO GET features/serving/latest/us/user-smith.json
               │
               ├─ hit  → backfill L1, return FeatureVector (10–50 ms)
               │
               └─ miss
                      │
                      ▼
                L3: Iceberg full scan (slow path, logs WARNING)
                      row_filter = "user_id = 'user-smith'"
                      → backfill L1 + L2, return FeatureVector (seconds)
                      → None if user genuinely absent
```

### mget() — batch optimisation

```
mget(["user-alpha", "user-beta", "user-gamma"])
        │
        ▼
  Redis MGET (single round-trip)
        │
        ├─ all hit → return immediately
        │
        └─ partial miss
               │
               ▼
         MinIO parallel GETs for misses (ThreadPoolExecutor in materialize.py)
               │
               └─ remaining misses → L3 Iceberg per user
```

---

## When to Use Which Path

| Scenario | Recommended path | Why |
|---|---|---|
| ML model training / backtesting | `BatchFeatureReader` (Iceberg) | Partition pruning reads only relevant days; Arrow → numpy is fast |
| Scheduled ETL / feature engineering | `BatchFeatureReader` (Iceberg or MinIO) | Horizontal date range; Parquet compression reduces bandwidth |
| Real-time model inference (<10 ms SLA) | `OnlineFeatureReader.get()` with Redis | L1 Redis < 2 ms; L2 MinIO 10–50 ms |
| Batch scoring (many users, single request) | `OnlineFeatureReader.mget()` | Redis MGET is O(1) round-trips regardless of batch size |
| Local dev / CI (no external services) | `BatchFeatureReader(backend="synthetic")` | Zero dependencies; deterministic data |
| Backfill serving layout after new training data | `online.materialize` | Reads offline → deduplicates → writes serving objects + warms Redis |

---

## Directory Structure

```
examples/feature-consumption/
├── shared/
│   ├── __init__.py
│   └── feature_vector.py       ← FeatureVector dataclass (used by all modules)
├── offline/
│   ├── __init__.py
│   └── batch_reader.py         ← BatchFeatureReader (Iceberg / MinIO / synthetic)
├── online/
│   ├── __init__.py
│   ├── online_reader.py        ← OnlineFeatureReader (Redis L1 / MinIO L2 / Iceberg L3)
│   └── materialize.py          ← offline → online materialization job
├── tests/
│   ├── __init__.py
│   ├── test_batch_reader.py    ← unit tests, no external services
│   └── test_online_reader.py   ← unit tests, no external services
├── docker-compose.yml          ← MinIO + Redis stack for local experimentation
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Start the serving stack

```bash
cd examples/feature-consumption
docker compose up -d
```

MinIO console: http://localhost:9001 (minioadmin / minioadmin)

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Read features offline (batch)

```bash
# Auto-detect backend (Iceberg → MinIO → synthetic):
python -m offline.batch_reader --start 2024-01-13 --end 2024-01-15

# Force synthetic (no external services):
python -m offline.batch_reader --backend synthetic --start 2024-01-15 --end 2024-01-15

# ML training dataset:
python -m offline.batch_reader --training --start 2024-01-01 --end 2024-01-07
```

### 4. Materialise the serving layout

```bash
# Yesterday's data → MinIO serving objects + Redis warm-up:
python -m online.materialize --days 1

# Specific date:
python -m online.materialize --date 2024-01-15

# Date range backfill:
python -m online.materialize --start 2024-01-01 --end 2024-01-07

# Skip Redis warm-up:
python -m online.materialize --days 1 --no-redis
```

### 5. Read features online (low-latency)

```bash
# Serial get() calls:
python -m online.online_reader user-smith john-doe unknown-user

# Batch mget():
python -m online.online_reader user-smith john-doe unknown-user --batch
```

### 6. Run the unit tests

```bash
# All tests (no external services needed):
python -m pytest tests/ -v

# Or with unittest:
python -m unittest discover tests/
```

---

## Environment Variables

All variables have safe defaults for local Docker Compose use.

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO / S3-compatible endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key |
| `MINIO_BUCKET` | `processed` | Bucket containing the serving layout |
| `MINIO_PREFIX` | `streamforge/features` | Prefix for raw offline JSON files |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis DB index |
| `REDIS_TTL_S` | `3600` | TTL for online reader cache entries |
| `MATERIALIZE_WORKERS` | `8` | Parallel MinIO PUT threads in materialize job |
| `WARM_REDIS` | `true` | Whether materialize warms Redis after writing MinIO |
| `ICEBERG_CATALOG_TYPE` | `hadoop` | Iceberg catalog: `hadoop`, `hive`, or `rest` |
| `ICEBERG_WAREHOUSE` | `s3a://streamforge/warehouse` | Iceberg warehouse location |
| `ICEBERG_DATABASE` | `streamforge` | Iceberg database / namespace |
| `ICEBERG_TABLE` | `user_event_counts` | Iceberg table name |
| `ICEBERG_S3_ENDPOINT` | _(empty)_ | Override S3 endpoint for Iceberg (MinIO) |

---

## Sharding Scheme

The serving layout uses a 2-character alphanumeric shard prefix derived from
the `user_id` to distribute objects across MinIO prefix namespaces:

```
_shard("user-smith")  → "us"   (first 2 alnum chars, lower-cased)
_shard("john-doe")    → "jo"
_shard("anonymous-1") → "an"
_shard("!")           → "xx"   (fallback when no alnum chars)
```

With 36² = 1296 possible shards, this distributes load evenly and avoids
S3/MinIO list-objects hot-spots at high write concurrency.


<!-- hobby-session-143 -->


<!-- hobby-session-199 -->


<!-- hobby-session-266 -->


<!-- hobby-session-31 -->
