/// <reference types="vite/client" />
import type {
  PipelinesResponse,
  FeaturesResponse,
  PipelineHealthResponse,
  FeatureHealthResponse,
  ViolationsResponse,
  EntityValuesResponse,
  EntityValueSummary,
  TraceResponse,
  EntityFeaturesResponse,
} from "./types";

const BASE = import.meta.env.VITE_API_BASE_URL ?? "/api";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function get<T>(
  path: string,
  params?: Record<string, string | number | boolean | undefined>,
  signal?: AbortSignal
): Promise<T> {
  const url = new URL(BASE + path, window.location.href);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) {
        url.searchParams.set(key, String(value));
      }
    }
  }
  const res = await fetch(url.toString(), { signal });
  if (!res.ok) {
    throw new ApiError(res.status, `HTTP ${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function fetchPipelines(signal?: AbortSignal): Promise<PipelinesResponse> {
  return get("/pipelines", undefined, signal);
}

export function fetchPipelineFeatures(pipelineId: string, signal?: AbortSignal): Promise<FeaturesResponse> {
  return get(`/pipelines/${encodeURIComponent(pipelineId)}/features`, undefined, signal);
}

export function fetchPipelineHealth(
  pipelineId: string,
  params: { window?: string; operator_id?: string },
  signal?: AbortSignal
): Promise<PipelineHealthResponse> {
  return get(`/pipelines/${encodeURIComponent(pipelineId)}/health`, params, signal);
}

export function fetchFeatureHealth(
  featureName: string,
  params: {
    pipeline_id: string;
    window?: string;
    compare_to?: string;
    operator_id?: string;
    end_time?: string;
  },
  signal?: AbortSignal
): Promise<FeatureHealthResponse> {
  return get(`/features/${encodeURIComponent(featureName)}/health`, params, signal);
}

export function fetchFeatureValues(
  featureName: string,
  params: {
    pipeline_id: string;
    window?: string;
    entity_id?: string;
    limit?: number;
    capture_mode?: string;
    operator_id?: string;
  },
  signal?: AbortSignal
): Promise<EntityValuesResponse> {
  return get(`/features/${encodeURIComponent(featureName)}/values`, params, signal);
}

export function fetchFeatureValueSummary(
  featureName: string,
  params: { pipeline_id: string; window?: string },
  signal?: AbortSignal
): Promise<EntityValueSummary> {
  return get(`/features/${encodeURIComponent(featureName)}/values/summary`, params, signal);
}

export function fetchViolations(
  params: {
    pipeline_id?: string;
    feature_name?: string;
    since?: string;
    severity_gte?: string;
    violation_type?: string;
    limit?: number;
    offset?: number;
  },
  signal?: AbortSignal
): Promise<ViolationsResponse> {
  return get("/violations", params, signal);
}

export function fetchTrace(traceId: string, signal?: AbortSignal): Promise<TraceResponse> {
  return get(`/traces/${encodeURIComponent(traceId)}`, undefined, signal);
}

export function fetchEntityFeatures(entityId: string, signal?: AbortSignal): Promise<EntityFeaturesResponse> {
  return get(`/entities/${encodeURIComponent(entityId)}/features`, undefined, signal);
}
