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
| `FARO_LOCAL_PATH` | `/var/faro/parquet` | Base directory for Parquet files (local backend) |
| `FARO_S3_BUCKET` | — | S3 bucket name (required if backend=s3) |
| `FARO_S3_PREFIX` | `faro/` | Key prefix within the bucket |
| `FARO_S3_REGION` | `us-east-1` | AWS region |
| `FARO_S3_ACCESS_KEY_ID` | — | AWS access key |
| `FARO_S3_SECRET_ACCESS_KEY` | — | AWS secret key |
| `FARO_S3_ENDPOINT_URL` | — | Custom endpoint (e.g. MinIO) |
| `FARO_FLUSH_INTERVAL_SECONDS` | `60` | How often the in-memory buffer is flushed to Parquet |
| `FARO_FLUSH_BUFFER_SIZE` | `1000` | Flush immediately if this many events are buffered |
| `FARO_DRIFT_THRESHOLD` | `0.20` | Fractional mean change that triggers a `MEAN_DRIFT` violation |
| `FARO_NULL_RATE_THRESHOLD` | `0.10` | Null fraction that triggers a `NULL_RATE` violation |
| `FARO_CARDINALITY_DROP_THRESHOLD` | `0.30` | Filter-ratio drop that triggers a `CARDINALITY_ANOMALY` violation |

## Endpoints

### POST /ingest

Ingest a capture event from `HttpCaptureEventSink`. The event is written to an in-memory
buffer and flushed to Parquet either on schedule (`FARO_FLUSH_INTERVAL_SECONDS`) or when
the buffer reaches `FARO_FLUSH_BUFFER_SIZE`.

**Request body** — `CaptureEvent` (JSON):

| Field | Type | Required | Description |
|---|---|---|---|
| `pipeline_id` | string | yes | Identifies the Flink pipeline |
| `operator_id` | string | yes | Flink operator name |
| `operator_type` | string | yes | e.g. `WINDOW`, `FILTER`, `MAP` |
| `capture_mode` | `AGGREGATE` \| `ENTITY` | yes | Aggregate row or per-entity row |
| `processing_time` | ISO-8601 string | yes | Wall-clock time of the capture |
| `trace_id` | string | yes | Distributed trace ID |
| `span_id` | string | yes | Span ID within the trace |
| `input_cardinality` | int | no | Number of input records |
| `output_cardinality` | int | no | Number of output records |
| `emit_interval_ms` | int | no | Expected emit interval; used for freshness detection |
| `capture_drop_since_last` | bool | no | Whether a drop was detected since the last capture |
| `feature_name` | string | no | Feature associated with this event |
| `entity_id` | string | no | Entity identifier (ENTITY mode) |
| `feature_value` | base64 bytes | no | Encoded feature value (ENTITY mode) |
| `feature_value_type` | string | no | e.g. `SCALAR_DOUBLE`, `SCALAR_LONG` |
| `event_time` | ISO-8601 string | no | Flink event time |
| `watermark` | ISO-8601 string | no | Current watermark |
| `parent_span_id` | string | no | Parent span for trace hierarchy |

**Response** — `{"status": "ok"}`

---

### GET /pipelines

List all pipeline IDs that have stored events.

**Response**:

```json
{
  "pipelines": ["pipe-1", "pipe-2"]
}
```

---

### GET /pipelines/{pipeline_id}/health

Per-operator cardinality and drop summary for a pipeline.

**Query parameters**:

| Parameter | Default | Description |
|---|---|---|
| `window` | `24h` | Time window to aggregate over. Accepts `Nh`, `Nm`, `Nd` |
| `operator_id` | — | Scope results to a single operator |

**Response** — `PipelineHealthResponse`:

```json
{
  "pipeline_id": "pipe-1",
  "operators": [
    {
      "operator_id": "filter-op",
      "operator_type": "FILTER",
      "last_seen": "2026-04-04T10:00:00+00:00",
      "total_input": 50000,
      "any_drops": false,
      "filter_ratio": 0.82
    }
  ]
}
```

`filter_ratio` is `avg(output_cardinality / input_cardinality)` for AGGREGATE events in the window. `null` when input cardinality is always zero.

---

### GET /pipelines/{pipeline_id}/features

List feature names observed for a pipeline.

**Response**:

```json
{
  "pipeline_id": "pipe-1",
  "features": ["temperature", "pressure", "voltage"]
}
```

---

### GET /features/{feature_name}/health

Cardinality trend, watermark lag, capture-drop flag, and violation signals for a feature.
Calling this endpoint also runs violation detection and writes any new violations to storage.

**Query parameters**:

| Parameter | Default | Description |
|---|---|---|
| `pipeline_id` | — | **Required.** Pipeline to query |
| `window` | `1h` | Time window. Accepts `Nh`, `Nm`, `Nd` |
| `compare_to` | — | Compare current window to a prior period, e.g. `24h_ago` |
| `operator_id` | — | Scope to a single operator |
| `end_time` | — | ISO-8601 upper bound on `processing_time` |

**Response** — `FeatureHealthResponse`:

```json
{
  "feature_name": "temperature",
  "pipeline_id": "pipe-1",
  "window": "1h",
  "cardinality_trend": [
    {
      "processing_time": "2026-04-04T10:00:00+00:00",
      "input_cardinality": 100,
      "output_cardinality": 82,
      "filter_ratio": 0.82,
      "watermark": "2026-04-04T09:59:00+00:00",
      "capture_drop_since_last": false
    }
  ],
  "watermark_lag_ms": 5432,
  "capture_drops": false,
  "emit_interval_ms": 30000,
  "freshness_violation": false,
  "comparison": null
}
```

`comparison` is populated when `compare_to` is set. `freshness_violation` is `true` when no
AGGREGATE event has arrived within `emit_interval_ms` of the current time. Violation records
for `FRESHNESS`, `MEAN_DRIFT`, `NULL_RATE`, and `CARDINALITY_ANOMALY` are written on each
call when the corresponding threshold is exceeded.

---

### GET /features/{feature_name}/values

Raw entity-level feature values within a time window.

**Query parameters**:

| Parameter | Default | Description |
|---|---|---|
| `pipeline_id` | — | **Required.** Pipeline to query |
| `window` | `1h` | Time window. Accepts `Nh`, `Nm`, `Nd` |
| `entity_id` | — | Filter to a single entity |
| `limit` | `10` | Maximum rows returned (1–10000) |
| `capture_mode` | `ENTITY` | `ENTITY`, `AGGREGATE`, or `ALL` |
| `operator_id` | — | Scope to a single operator |

**Response** — `EntityValuesResponse`:

```json
{
  "feature_name": "temperature",
  "pipeline_id": "pipe-1",
  "window": "1h",
  "values": [
    {
      "entity_id": "device-7",
      "feature_value_decoded": 98.6,
      "feature_value_type": "SCALAR_DOUBLE",
      "processing_time": "2026-04-04T10:01:00+00:00",
      "event_time": "2026-04-04T10:00:58+00:00"
    }
  ]
}
```

`feature_value_decoded` is the numeric value unpacked from the binary `feature_value` field.
`null` when the raw value is absent or of an unsupported type.

---

### GET /features/{feature_name}/values/summary

Statistical summary of entity feature values within a time window.

**Query parameters**:

| Parameter | Default | Description |
|---|---|---|
| `pipeline_id` | — | **Required.** Pipeline to query |
| `window` | `1h` | Time window. Accepts `Nh`, `Nm`, `Nd` |

**Response** — `EntityValueSummary`:

```json
{
  "feature_name": "temperature",
  "pipeline_id": "pipe-1",
  "window": "1h",
  "entity_count": 512,
  "value_min": 36.1,
  "value_max": 102.4,
  "value_mean": 72.3,
  "value_p50": 71.8,
  "value_p95": 99.1,
  "null_count": 4
}
```

Only ENTITY events with a decodable `feature_value` contribute to the statistics.
`null_count` counts ENTITY rows where `feature_value` is absent.

---

### GET /violations

Recorded violations with filtering and pagination.

**Query parameters**:

| Parameter | Default | Description |
|---|---|---|
| `pipeline_id` | — | Filter to a single pipeline |
| `feature_name` | — | Filter to a single feature |
| `since` | — | ISO-8601 lower bound on `detected_at` |
| `severity_gte` | — | Minimum severity: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `violation_type` | — | Filter by type: `FRESHNESS`, `MEAN_DRIFT`, `NULL_RATE`, `CARDINALITY_ANOMALY` |
| `limit` | `100` | Page size (1–1000) |
| `offset` | `0` | Page offset |

**Response** — `ViolationsResponse`:

```json
{
  "violations": [
    {
      "pipeline_id": "pipe-1",
      "feature_name": "temperature",
      "violation_type": "NULL_RATE",
      "detected_at": "2026-04-04T10:05:00+00:00",
      "severity": "HIGH",
      "detail": "Feature 'temperature' null rate exceeds threshold"
    }
  ],
  "total": 47
}
```

`total` is the total number of rows matching the filters, regardless of `limit`/`offset`.
Violations are written by `GET /features/{name}/health` when thresholds are exceeded.

---

### GET /traces/{trace_id}

All capture events for a given trace ID, across all pipelines and operators.

**Response** — `TraceResponse`:

```json
{
  "trace_id": "abc-123",
  "events": [
    {
      "pipeline_id": "pipe-1",
      "operator_id": "filter-op",
      "operator_type": "FILTER",
      "feature_name": "temperature",
      "capture_mode": "AGGREGATE",
      "processing_time": "2026-04-04T10:00:00+00:00",
      "trace_id": "abc-123",
      "span_id": "span-1",
      "parent_span_id": null,
      "input_cardinality": 100,
      "output_cardinality": 82
    }
  ]
}
```

---

### GET /entities/{entity_id}/features

Latest feature value per `(pipeline_id, feature_name)` for a given entity, across all
pipelines.

**Response** — `EntityFeaturesResponse`:

```json
{
  "entity_id": "device-7",
  "features": [
    {
      "pipeline_id": "pipe-1",
      "feature_name": "temperature",
      "feature_value_decoded": 98.6,
      "processing_time": "2026-04-04T10:01:00+00:00"
    }
  ]
}
```

---

## Violation Detection

Violation detection runs on every `GET /features/{name}/health` call. Four types are
detected:

| Type | Trigger | Severity |
|---|---|---|
| `FRESHNESS` | No AGGREGATE event received within `emit_interval_ms` of now | HIGH |
| `MEAN_DRIFT` | Mean feature value in current window deviates from prior window by more than `FARO_DRIFT_THRESHOLD` (default 20%) | MEDIUM |
| `NULL_RATE` | Fraction of ENTITY rows with null `feature_value` exceeds `FARO_NULL_RATE_THRESHOLD` (default 10%) | HIGH |
| `CARDINALITY_ANOMALY` | Average output/input ratio drops by more than `FARO_CARDINALITY_DROP_THRESHOLD` (default 30%) compared to the prior window | HIGH |

---

## Storage

Events are stored as Parquet files partitioned as:

```
{base}/pipeline_id={value}/date={yyyy-MM-dd}/part-{uuid}.parquet
```

Violations are stored under `{base}/violations/pipeline_id={value}/`. DuckDB reads all
files at query time via `read_parquet(glob)` — there is no persistent database file.

## Tests

```bash
uv run python -m pytest
```
