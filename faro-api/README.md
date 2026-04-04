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

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest` | Ingest a capture event from `HttpCaptureEventSink` |
| `GET` | `/pipelines` | List all known pipeline IDs |
| `GET` | `/pipelines/{pipeline_id}/health` | Per-operator cardinality and drop summary |
| `GET` | `/pipelines/{pipeline_id}/features` | List features observed for a pipeline |
| `GET` | `/features/{feature_name}/health` | Feature cardinality trend, watermark lag, and violation signals |
| `GET` | `/features/{feature_name}/values` | Raw entity-level feature values |
| `GET` | `/features/{feature_name}/values/summary` | Statistical summary of entity values (min, max, mean, percentiles) |
| `GET` | `/violations` | Recorded violations with filtering and pagination |
| `GET` | `/traces/{trace_id}` | All capture events for a trace ID |
| `GET` | `/entities/{entity_id}/features` | Latest feature value per pipeline and feature for an entity |

## Storage

Events are stored as Parquet files partitioned as:

```
{base}/pipeline_id={value}/date={yyyy-MM-dd}/part-{uuid}.parquet
```

Violations are stored under `{base}/violations/pipeline_id={value}/`. DuckDB reads all
files at query time via `read_parquet(glob)` — there is no persistent database file.

## Tests

```bash
uv run --extra dev pytest
```
