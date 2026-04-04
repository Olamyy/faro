export interface PipelinesResponse {
  pipelines: string[];
}

export interface FeaturesResponse {
  pipeline_id: string;
  features: string[];
}

export interface OperatorSummary {
  operator_id: string;
  operator_type: string;
  last_seen: string | null;
  total_input: number;
  any_drops: boolean;
  filter_ratio: number | null;
}

export interface PipelineHealthResponse {
  pipeline_id: string;
  operators: OperatorSummary[];
}

export interface CardinalityPoint {
  processing_time: string;
  input_cardinality: number;
  output_cardinality: number;
  filter_ratio: number | null;
  watermark: string | null;
  capture_drop_since_last: boolean;
}

export interface FeatureHealthResponse {
  feature_name: string;
  pipeline_id: string;
  window: string;
  cardinality_trend: CardinalityPoint[];
  watermark_lag_ms: number | null;
  capture_drops: boolean;
  emit_interval_ms: number | null;
  freshness_violation: boolean;
  comparison: Record<string, unknown> | null;
}

export interface ComparisonData {
  period: string;
  avg_input_cardinality: number;
  avg_output_cardinality: number;
  any_drops: boolean;
}

export interface Violation {
  pipeline_id: string;
  feature_name: string | null;
  violation_type: string;
  detected_at: string;
  severity: string;
  detail: string;
}

export interface ViolationsResponse {
  violations: Violation[];
  total: number;
}

export interface EntityValuePoint {
  entity_id: string | null;
  feature_value_decoded: number | string | null;
  feature_value_type: string | null;
  processing_time: string;
  event_time: string | null;
}

export interface EntityValuesResponse {
  feature_name: string;
  pipeline_id: string;
  window: string;
  values: EntityValuePoint[];
}

export interface EntityValueSummary {
  feature_name: string;
  pipeline_id: string;
  window: string;
  entity_count: number;
  value_min: number | null;
  value_max: number | null;
  value_mean: number | null;
  value_p50: number | null;
  value_p95: number | null;
  null_count: number;
}

export interface TraceEvent {
  pipeline_id: string;
  operator_id: string;
  operator_type: string;
  feature_name: string | null;
  capture_mode: "AGGREGATE" | "ENTITY";
  processing_time: string;
  trace_id: string;
  span_id: string;
  parent_span_id: string | null;
  input_cardinality: number;
  output_cardinality: number;
}

export interface TraceResponse {
  trace_id: string;
  events: TraceEvent[];
}

export interface EntityFeaturePoint {
  pipeline_id: string;
  feature_name: string;
  feature_value_decoded: number | string | null;
  processing_time: string;
}

export interface EntityFeaturesResponse {
  entity_id: string;
  features: EntityFeaturePoint[];
}
