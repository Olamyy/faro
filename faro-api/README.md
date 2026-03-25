# faro-api

Ingest and query layer for Faro capture events. Receives events from instrumented Flink
pipelines via HTTP, stores them as Parquet files, and exposes a REST API for querying
pipeline and feature health.

## Running

```bash
uvicorn api.main:app --host 0.0.0.0 --port 9000
```

Or via Docker Compose — see `faro-e2e/docker-compose.yml`.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FARO_STORAGE_BACKEND` | `local` | `local` or `s3` |
| `FARO_LOCAL_PATH` | `/var/faro/parquet` | Base directory for Parquet files |
| `FARO_S3_BUCKET` | — | S3 bucket name (required if backend=s3) |
| `FARO_S3_PREFIX` | `faro/` | Key prefix within the bucket |
| `FARO_S3_ENDPOINT_URL` | — | Optional custom endpoint (e.g. MinIO) |
| `FARO_FLUSH_INTERVAL_SECONDS` | `60` | How often the in-memory buffer is flushed to Parquet |
| `FARO_FLUSH_BUFFER_SIZE` | `1000` | Flush immediately if this many events are buffered |

## Endpoints

### `POST /ingest`

Accepts a `CaptureEvent` JSON body from `HttpCaptureEventSink`. Always returns `202
Accepted` — errors are logged but never returned to the caller, consistent with how the
sink handles failures (it logs and continues).

Events are held in memory and flushed to Parquet in batches, partitioned as:

```
{base}/pipeline_id={value}/date={yyyy-MM-dd}/part-{uuid}.parquet
```

---

### `GET /features/{feature_name}/health`

Returns health signals for a single feature over a time window.

**Query parameters**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `pipeline_id` | yes | — | Pipeline to query |
| `window` | no | `1h` | How far back to look. Accepts `30m`, `1h`, `7d`, etc. |
| `compare_to` | no | — | Shift the same window back in time for comparison, e.g. `24h_ago` |

**Response fields**

| Field | Description |
|---|---|
| `cardinality_trend` | One entry per capture event in the window. Each entry records `input_cardinality`, `output_cardinality`, `watermark`, and `capture_drop_since_last` at that point in time. Use this to spot where records are being dropped or filtered across the pipeline. |
| `watermark_lag_ms` | How far the pipeline's watermark is behind wall-clock time, in milliseconds. A high value means the pipeline is processing data that is late relative to real time, or has stalled. |
| `capture_drops` | `true` if any capture event in the window had `capture_drop_since_last = true`. This means the `AsyncCaptureEventSink` ring buffer was full and some observability events were discarded. Note: this is about lost *observability* events, not lost pipeline data. |
| `freshness_violation` | `true` if no capture event for this feature has arrived in the last `emit_interval_ms * 3`. Indicates the feature has gone silent — the pipeline may have stalled, crashed, or the sink may be failing. |
| `comparison` | Cardinality stats for the same-length window shifted back by `compare_to`. Useful for comparing today vs yesterday. `null` if `compare_to` was not supplied. |

**What is and is not checked today**

`freshness_violation` is the only active check — it evaluates whether an event arrived
within the expected window. Everything else (`cardinality_trend`, `watermark_lag_ms`,
`capture_drops`) is informational: it is stored and returned but does not trigger any
alert. Value-level checks (`value_mean`, `value_p95`, `null_count`, etc.) are not yet
implemented.

---

### `GET /pipelines/{pipeline_id}/health`

Returns one row per operator in the pipeline, aggregated across all stored events.

**Response fields**

| Field | Description |
|---|---|
| `operator_id` | Stable UID set via `.uid("...")` in the Flink job. Consistent across restarts, so you can track the same operator over time. |
| `operator_type` | The kind of operator: `WINDOW`, `SINK`, `MAP`, `FILTER`, etc. |
| `last_seen` | `processing_time` of the most recent capture event from this operator. If this timestamp is stale, the operator has stopped emitting. |
| `total_input` | Sum of `input_cardinality` across all captured intervals. A rough measure of cumulative throughput through this operator. |
| `any_drops` | `true` if any capture event from this operator ever had `capture_drop_since_last = true`. |

---

### `GET /violations`

Returns recorded violations. A violation is written when a health check detects a
problem — currently only when `GET /features/{name}/health` detects a freshness
violation.

**Query parameters**

| Parameter | Description |
|---|---|
| `pipeline_id` | Filter to one pipeline |
| `feature_name` | Filter to one feature |
| `since` | ISO-8601 lower bound on `detected_at` |
| `severity_gte` | Minimum severity: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |

**Response fields**

| Field | Description |
|---|---|
| `violation_type` | What kind of problem was detected. Currently only `FRESHNESS`. |
| `severity` | `LOW`, `MEDIUM`, `HIGH`, or `CRITICAL`. Freshness violations are always `HIGH`. |
| `detected_at` | Wall-clock time the violation was detected — i.e. when the health endpoint was queried and found the feature stale. Not when the pipeline actually went silent. |
| `detail` | Human-readable description of what triggered the violation. |

**Violation types (phase 4 scope)**

| Type | Meaning |
|---|---|
| `FRESHNESS` | No capture event received for the feature within `emit_interval_ms * 3`. The only type currently implemented. |

Value-level violation types (e.g. mean drift, null rate spike, cardinality anomaly) are
not yet implemented.

## Storage

Events are stored as Parquet files using pyarrow. DuckDB reads them at query time via
`read_parquet(glob)` — there is no persistent DuckDB database file. Violations are stored
in a separate `violations/pipeline_id={value}/` partition under the same base path.

## Tests

```bash
uv run --extra dev pytest
```
