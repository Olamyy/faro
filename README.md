# Faro

Faro is the simplest feature observability layer for streaming engines you can think of.
It's entirely passive and non-intrusive. You wrap your existing operators with Faro's
decorators, configure which features to track, and capture events start flowing alongside
your data. You can route those events to wherever you already send observability data:
<a href="https://grafana.com" target="_blank">Grafana</a>,
<a href="https://lightstep.com" target="_blank">Lightstep</a>,
<a href="https://www.honeycomb.io" target="_blank">Honeycomb</a>,
or your existing Kafka topics. 

> [!NOTE]
> 
> If you don't have a monitoring stack, faro includes a `faro-api` REST service that provides a Parquet-backed storage and a query layer.

*Faro exists because feature pipelines are production infrastructure and deserve the same observability as your API servers.*

**Apache Flink** is the only engine with an adapter today. Spark Structured Streaming and
Kafka Streams adapters are on the roadmap.

## What problem does it solve?

Feature pipeline observability today happens at rest. You query your data warehouse, look at
model metrics, or wait for a downstream alert. When a feature value is off, you have almost no visibility into
when the feature went bad, which operator dropped records, or what value a specific entity
had at processing time. The gap is in the pipeline itself. Streaming engines process millions of records per second and
emit nothing observable by default. You find out something is wrong when:

- A downstream model starts producing bad predictions.
- A user reports that their feature value is stale or missing.
- An SLA alert fires, and you have no idea when the pipeline actually went silent.

Faro closes that gap by making the inside of your pipeline visible at runtime, per operator,
per entity, without touching your pipeline logic.

## Installation

### Flink (Java)

Add `faro-flink` to your build. This requires **Flink 1.18.x** and **Java 17**. `faro-flink` declares Flink as `compileOnly`, so it depends on your existing flink runtime without bundling Flink by itself.


```gradle
dependencies {
    implementation 'dev.faro:faro-flink:0.1.0-SNAPSHOT'
}
```

### faro-api (optional)

If you don't have an existing monitoring stack:

```bash
docker run -p 9000:9000 \
  -e FARO_LOCAL_PATH=/var/faro/parquet \
  -v faro-data:/var/faro/parquet \
  faro-api
```

## Comparison

There are many tools on the market that can provide some level of feature observability, but they all require significant instrumentation, custom metrics, or new infrastructure.
Faro wants to be really simple. It provides more out-of-the-box visibility with zero pipeline changes and no new infrastructure required.

**Observability tools**

| | Faro | Datadog | Grafana | Lightstep | Honeycomb |
|---|---|---|---|---|---|
| Pipeline-level cardinality & watermark | ✓ | needs custom metrics | needs custom metrics | needs custom metrics | needs custom spans |
| Per-entity feature value at processing time | ✓ | ✗ | ✗ | ✗ | ✗ |
| Freshness violations | ✓ | needs alerting rules | needs alerting rules | needs alerting rules | needs alerting rules |
| Zero pipeline changes required | ✓ | ✗ | ✗ | ✗ | ✗ |
| Works without new infra | ✓ (stdout/Kafka/OTLP) | ✗ | ✗ | ✗ | ✗ |

**Feature stores**

| | Faro | Hopsworks | SageMaker Feature Store | Tecton | Feast | Databricks Feature Store |
|---|---|---|---|---|---|---|
| Pipeline-level cardinality & watermark | ✓ | ✗ | ✗ | partial | ✗ | ✗ |
| Per-entity feature value at processing time | ✓ | serving store only | serving store only | serving store only | serving store only | serving store only |
| Freshness violations | ✓ | partial | partial | ✓ | ✗ | partial |
| Zero pipeline changes required | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Works without new infra | ✓ (stdout/Kafka/OTLP) | ✗ | ✗ | ✗ | ✗ | ✗ |

## High-level operations

Every instrumented operator emits two kinds of events:

**AGGREGATE events** fire on each flush (window close, periodic timer). They capture input
and output cardinality, watermark, event-time range, late event count, and whether any
observability events were dropped since last flush.

**ENTITY events** fire per record, when you configure a feature with an entity key and value
extractor. They capture the entity ID, the raw feature value at processing time, and event
time. Features classified as `PERSONAL` or `SENSITIVE` are automatically suppressed. They
degrade to AGGREGATE mode and no entity data is ever emitted.

A *pipeline* is a named unit containing one or more instrumented *operators*. Each operator
must have a stable UID (via `.uid("...")` in Flink) so Faro can correlate events across
restarts. Events flow from operators into a *sink*, which routes them to your destination of
choice.

## Components

| Module | Description                                                                                                                                                                                                                                              |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `faro-core` | `CaptureEvent` model with JSON and Avro serialization. Engine-agnostic with no adapter dependencies.                                                                                                                                                     |
| `faro-flink` | Flink adapter that wraps operators and sink implementations.                                                                                                                                                                                             |
| `faro-api` | A FastAPI service that exposes two endpoints. An `/ingest` endpoint that buffers incoming `CaptureEvent`s and flushes them to Parquet, and `/query` layer backed by DuckDB for exploring feature health, pipeline health, entity values, and violations. |
| `faro-e2e` | Runnable demo Flink jobs covering every sink variant and both AGGREGATE and ENTITY modes.                                                                                                                                                                |

**Sink options:**

| Sink | When to use                                                     |
|------|-----------------------------------------------------------------|
| `StdoutCaptureEventSink` | Local development and testing                                   |
| `KafkaCaptureEventSink` | You already have Kafka or Redpanda                              |
| `HttpCaptureEventSink` | You want faro-api, or any webhook receiver                      |
| `OtelCaptureEventSink` | You already have Grafana, Lightstep, or Honeycomb               |
| `AsyncCaptureEventSink` | Wraps any of the above. Decouples capture from operator threads |

All sinks are fire-and-forget. Failures are logged and never propagate to your pipeline.
`AsyncCaptureEventSink` additionally tracks overflow. When the in-memory ring buffer fills
up, it sets a `capture_drop_since_last` flag on the next flush event so you can see it in
the query layer.

## Example

### Aggregate mode

Track cardinality and watermark health for a window operator. No per-entity data is captured.

```java
Faro faro = new Faro("order-pipeline",
    AsyncCaptureEventSink.wrap(HttpCaptureEventSink.factory("http://faro-api:9000/ingest"), 1000));

// Register features in AGGREGATE mode only.
FaroConfig<OrderEvent> config = FaroConfig.<OrderEvent>builder()
    .features("order_count", "revenue_7d")
    .build();

DataStream<OrderSummary> output = input
    .keyBy(e -> e.merchantId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .process(faro.windowTrace(new OrderAggFn(), config))
    .uid("order-agg-window");
```

Query pipeline and feature health:

```bash
# Is the pipeline flowing? When did each operator last emit?
curl "http://localhost:9000/pipelines/order-pipeline/health"

# Has cardinality dropped in the last hour? Is the watermark lagging?
curl "http://localhost:9000/features/order_count/health?pipeline_id=order-pipeline&window=1h"

# Compare throughput now vs 24 hours ago
curl "http://localhost:9000/features/order_count/health?pipeline_id=order-pipeline&window=1h&compare_to=24h_ago"

# Any freshness violations?
curl "http://localhost:9000/violations?pipeline_id=order-pipeline&severity_gte=HIGH"
```

### Entity mode

Capture the feature value for each entity at processing time. Useful for debugging model inputs
and auditing what a specific user or device saw.

```java
Faro faro = new Faro("user-feature-pipeline",
    AsyncCaptureEventSink.wrap(HttpCaptureEventSink.factory("http://faro-api:9000/ingest"), 1000));

FaroConfig<PurchaseEvent> config = FaroConfig.<PurchaseEvent>builder()
    .feature("purchase_amount_7d", FaroFeatureConfig.<PurchaseEvent>builder()
        .entityKey(e -> e.userId)
        .featureValue(e -> e.rollingAmount)
        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
        .classification(DataClassification.NON_PERSONAL)
        .sampleRate(0.1)   // capture entity events for 10% of records
        .build())
    .build();

DataStream<FeatureVector> output = input
    .keyBy(e -> e.userId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .process(faro.windowTrace(new RollingAmountFn(), config))
    .uid("rolling-amount-window");
```

Query entity values and distribution stats:

```bash
# What value did this user have in the last hour?
curl "http://localhost:9000/features/purchase_amount_7d/values?pipeline_id=user-feature-pipeline&entity_id=user-42&window=1h"

# Distribution across all entities: min, max, mean, p50, p95
curl "http://localhost:9000/features/purchase_amount_7d/values/summary?pipeline_id=user-feature-pipeline&window=1h"
```

## Roadmap

**Engine adapters**

Flink is the only supported engine today. Spark Structured Streaming and Kafka Streams adapters are planned.

**Extended query API**

The current query layer covers cardinality trends, watermark lag, freshness violations, and entity values. Planned additions include cardinality drop detection, output/input filter-ratio trends, late-event rate per window, missing window detection, and per-window fire-delay distribution. Cross-operator queries (cardinality funnel, inter-operator lag) are also planned but blocked on wiring `parent_span_id` at emit time.

**Value-level health checks**

The violation system currently detects freshness only. Planned checks include null rate spikes, mean drift, cardinality anomalies (Z-score over a rolling 7-day baseline), and NOT_NULL / RANGE assertions in entity mode.

**Entity mode and lineage**

Full entity-level lineage including bitemporal indexing, per-entity erasure, and a hydrated DAG overlay are all scoped for a later phase after the aggregate-mode foundation is proven against real pipelines.

**Built-in UI**

A lightweight dashboard served directly by `faro-api` is planned, covering pipeline and feature health at a glance, cardinality and watermark trends over configurable windows, a violations feed, and an entity value explorer. The goal is to give teams without an existing observability stack a usable interface out of the box, with no separate deployment required.

---

## License and links

- License: Apache-2.0
