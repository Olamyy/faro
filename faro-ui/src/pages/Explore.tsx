import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  fetchPipelines, fetchPipelineFeatures, fetchFeatureHealth,
  fetchFeatureValues, fetchFeatureValueSummary, fetchViolations,
  fetchPipelineHealth, fetchEntityFeatures, fetchTrace,
} from "../api/client";
import type {
  FeatureHealthResponse, EntityValuesResponse, EntityValueSummary,
  ViolationsResponse, PipelineHealthResponse, EntityFeaturesResponse, TraceResponse,
  ComparisonData,
} from "../api/types";
import { StatusBadge } from "../components/StatusBadge";
import { WindowSelector } from "../components/WindowSelector";

function fmtMs(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3_600_000) return `${(ms / 60_000).toFixed(1)}m`;
  if (ms < 86_400_000) return `${(ms / 3_600_000).toFixed(1)}h`;
  return `${(ms / 86_400_000).toFixed(1)}d`;
}

function fmtTime(iso: string) {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

function StatCard({ label, value, red, tooltip }: { label: string; value: string; red?: boolean; tooltip?: string }) {
  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-3">
      <div className="flex items-center justify-between mb-1">
        <p className="text-xs text-zinc-500">{label}</p>
        {tooltip && (
          <div className="group relative flex items-center">
            <svg className="w-3.5 h-3.5 text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-300 cursor-help" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="10" strokeWidth="2" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 16v-4m0-4h.01" />
            </svg>
            <div className="absolute right-0 top-5 z-20 hidden group-hover:block w-64 rounded-lg border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 px-3 py-2.5 text-xs text-zinc-600 dark:text-zinc-300 shadow-lg leading-relaxed">
              {tooltip}
            </div>
          </div>
        )}
      </div>
      <p className={`text-lg font-semibold tabular-nums ${red ? "text-red-500" : ""}`}>{value}</p>
    </div>
  );
}

function ChartHeader({ title, legend, tooltip, warning }: {
  title: string;
  legend?: { color: string; label: string }[];
  tooltip: string;
  warning?: string;
}) {
  return (
    <div className="flex items-center justify-between mb-1">
      <div className="flex items-center gap-4">
        <span className="text-xs font-medium text-zinc-600 dark:text-zinc-400">{title}</span>
        {legend && (
          <div className="flex gap-3">
            {legend.map((l) => (
              <span key={l.label} className="flex items-center gap-1.5 text-xs text-zinc-500">
                <span className="w-2.5 h-2.5 rounded-sm inline-block" style={{ background: l.color }} />
                {l.label}
              </span>
            ))}
          </div>
        )}
      </div>
      <div className="flex items-center gap-3">
        {warning && <span className="text-xs text-red-500 font-medium">{warning}</span>}
        <div className="group relative flex items-center">
          <svg className="w-3.5 h-3.5 text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-300 cursor-help" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <circle cx="12" cy="12" r="10" strokeWidth="2" />
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 16v-4m0-4h.01" />
          </svg>
          <div className="absolute right-0 top-5 z-20 hidden group-hover:block w-72 rounded-lg border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 px-3 py-2.5 text-xs text-zinc-600 dark:text-zinc-300 shadow-lg leading-relaxed">
            {tooltip}
          </div>
        </div>
      </div>
    </div>
  );
}

function Empty({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={`flex items-center justify-center py-16 text-sm text-zinc-400 ${className}`}>
      {children}
    </div>
  );
}

function CardinalityTooltip({ active, payload, label }: { active?: boolean; payload?: { dataKey: string; name: string; value: number; color: string }[]; label?: string }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-700 rounded-lg px-3 py-2 text-xs shadow">
      <p className="text-zinc-500 font-medium mb-1">{label ?? ""}</p>
      {payload.map((p) => (
        <p key={p.dataKey} className="my-0.5" style={{ color: p.color }}>
          {p.name}: {p.value.toLocaleString()}
        </p>
      ))}
    </div>
  );
}

function FilterRatioTooltip({ active, payload, label }: { active?: boolean; payload?: { dataKey: string; value: number }[]; label?: string }) {
  if (!active || !payload?.length) return null;
  const fr = payload.find((p) => p.dataKey === "FilterRatio");
  const drop = payload.find((p) => p.dataKey === "CaptureDrop");
  return (
    <div className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-700 rounded-lg px-3 py-2 text-xs shadow">
      <p className="text-zinc-500 font-medium mb-1">{label}</p>
      {fr?.value != null ? <p style={{ color: "#ff9830" }}>filter ratio: {fr.value}%</p> : <p className="text-zinc-400">no filter ratio data</p>}
      {drop?.value != null && <p className="text-red-500">capture drop</p>}
    </div>
  );
}

function WatermarkTooltip({ active, payload, label }: { active?: boolean; payload?: { value: number | null }[]; label?: string }) {
  if (!active || !payload?.length) return null;
  const v = payload[0]?.value as number | null;
  return (
    <div className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-700 rounded-lg px-3 py-2 text-xs shadow">
      <p className="text-zinc-500 font-medium mb-1">{label}</p>
      {v != null ? <p style={{ color: "#b877d9" }}>lag: {fmtMs(v)}</p> : <p className="text-zinc-400">no watermark data</p>}
    </div>
  );
}

function FeatureHealthPanel({ pipeline, feature, timeWindow, compareTo, onCompareChange }: { pipeline: string; feature: string; timeWindow: string; compareTo: string; onCompareChange: (v: string) => void }) {
  const [data, setData] = useState<FeatureHealthResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!pipeline || !feature) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchFeatureHealth(feature, { pipeline_id: pipeline, window: timeWindow, compare_to: compareTo || undefined }, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to load feature health."); })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [pipeline, feature, timeWindow, compareTo]);

  if (!pipeline || !feature) return <Empty>Select a pipeline and feature above.</Empty>;
  if (loading) return <Empty>Loading…</Empty>;
  if (error) return <Empty className="text-red-500">{error}</Empty>;
  if (!data) return null;

  const chartData = data.cardinality_trend
    .map((p) => {
      const ts = new Date(p.processing_time).getTime();
      const wmLagMs = p.watermark ? Math.max(0, ts - new Date(p.watermark).getTime()) : null;
      return {
        ts,
        time: fmtTime(p.processing_time),
        Input: p.input_cardinality,
        Output: p.output_cardinality,
        FilterRatio: p.filter_ratio !== null ? Math.round(p.filter_ratio * 1000) / 10 : null,
        CaptureDrop: p.capture_drop_since_last ? 1 : null,
        WatermarkLagMs: wmLagMs,
      };
    })
    .sort((a, b) => a.ts - b.ts);

  const comparison = (data.comparison && "period" in data.comparison) ? data.comparison as unknown as ComparisonData : null;

  const gapTime: string | null = (() => {
    if (chartData.length < 3) return null;
    const diffs = chartData.slice(1).map((d, i) => d.ts - chartData[i].ts);
    const sorted = [...diffs].sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];
    const gapIdx = diffs.findIndex((d) => d > median * 4);
    return gapIdx >= 0 ? chartData[gapIdx + 1].time : null;
  })();

  const hasAlerts = data.freshness_violation || data.capture_drops;

  return (
    <div className="flex flex-col gap-5">
      {hasAlerts && (
        <div className="rounded-lg border border-red-200 dark:border-red-900 bg-red-50 dark:bg-red-950/30 px-4 py-3 flex flex-col gap-2">
          {data.freshness_violation && (
            <div className="flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-red-500 shrink-0" />
              <span className="text-sm text-red-700 dark:text-red-400 font-medium">Freshness violation</span>
              <span className="text-xs text-red-500">— feature has not emitted within the expected interval</span>
            </div>
          )}
          {data.capture_drops && (
            <div className="flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-red-500 shrink-0" />
              <span className="text-sm text-red-700 dark:text-red-400 font-medium">Capture drops</span>
              <span className="text-xs text-red-500">— events were lost before processing in this window</span>
            </div>
          )}
        </div>
      )}

      <div className="grid grid-cols-3 gap-3">
        <StatCard label="Watermark lag" value={fmtMs(data.watermark_lag_ms)} red={data.watermark_lag_ms !== null && data.watermark_lag_ms > 30_000} tooltip="Time between the most recent event's timestamp and when it was processed. High lag (>30s) indicates the pipeline is falling behind real-time ingestion." />
        <StatCard label="Emit interval" value={fmtMs(data.emit_interval_ms)} tooltip="How frequently this feature emits values. Derived from the median gap between consecutive processing times in the selected window." />
        <StatCard label="Data points" value={String(data.cardinality_trend.length)} tooltip="Number of cardinality snapshots returned by the API for the selected window. Each point represents one aggregation interval." />
      </div>

      <div className="flex items-center gap-2">
        <span className="text-xs text-zinc-500">Compare to</span>
        <select
          value={compareTo}
          onChange={(e) => onCompareChange(e.target.value)}
          className="rounded border border-zinc-300 dark:border-zinc-600 bg-white dark:bg-zinc-800 text-xs px-2 py-1 text-zinc-600 dark:text-zinc-300"
        >
          <option value="">none</option>
          <option value="24h_ago">24h ago</option>
          <option value="7d_ago">7d ago</option>
        </select>
      </div>

      {chartData.length === 0 ? (
        <Empty>No cardinality data in this window.</Empty>
      ) : (
        <>
          <ChartHeader
            title="Cardinality trend"
            legend={[{ color: "#378ADD", label: "input" }, { color: "#1D9E75", label: "output" }]}
            tooltip="Number of records entering (input) and leaving (output) this feature's operator each processing cycle. A persistent gap between the two means records are being filtered or dropped."
          />
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={chartData} margin={{ top: 4, right: 16, left: 8, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(136,135,128,0.2)" />
              <XAxis dataKey="time" tick={{ fontSize: 11, fill: "#888780" }} interval={Math.max(1, Math.floor(chartData.length / 7))} tickLine={false} axisLine={false} />
              <YAxis domain={["auto", "auto"]} tick={{ fontSize: 11, fill: "#888780" }} tickFormatter={(v: number) => v.toLocaleString()} tickLine={false} axisLine={false} width={52} />
              <Tooltip content={<CardinalityTooltip />} />
              {gapTime !== null && (
                <ReferenceLine x={gapTime} stroke="#888780" strokeDasharray="4 3" strokeWidth={1} label={{ value: "gap", position: "top", fontSize: 10, fill: "#888780" }} />
              )}
              <Line type="linear" dataKey="Input" name="input cardinality" stroke="#378ADD" strokeWidth={1.5} dot={{ r: 2, fill: "#378ADD" }} activeDot={{ r: 4 }} isAnimationActive={false} />
              <Line type="linear" dataKey="Output" name="output cardinality" stroke="#1D9E75" strokeWidth={1.5} dot={{ r: 2, fill: "#1D9E75" }} activeDot={{ r: 4 }} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>

          <ChartHeader
            title="Filter ratio"
            legend={[{ color: "#ff9830", label: "% passed" }, ...(chartData.some((d) => d.CaptureDrop) ? [{ color: "#f2495c", label: "capture drop" }] : [])]}
            tooltip="Percentage of input records that pass through to output each cycle (output ÷ input × 100). A sudden dip signals increased filtering, a bug, or data quality issues. Red dots mark cycles where a capture drop was recorded."
            warning={chartData.some((d) => d.CaptureDrop) ? "capture drops detected" : undefined}
          />
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={chartData} margin={{ top: 4, right: 16, left: 8, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(136,135,128,0.2)" />
              <XAxis dataKey="time" tick={{ fontSize: 11, fill: "#888780" }} interval={Math.max(1, Math.floor(chartData.length / 7))} tickLine={false} axisLine={false} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 11, fill: "#888780" }} tickFormatter={(v: number) => `${v}%`} tickLine={false} axisLine={false} width={52} ticks={[0, 25, 50, 75, 100]} />
              <Tooltip content={<FilterRatioTooltip />} />
              <ReferenceLine y={100} stroke="rgba(136,135,128,0.3)" strokeDasharray="4 3" />
              <Line type="linear" dataKey="FilterRatio" name="filter ratio" stroke="#ff9830" strokeWidth={1.5} dot={{ r: 2, fill: "#ff9830" }} activeDot={{ r: 4 }} isAnimationActive={false} connectNulls={false} />
              <Line type="linear" dataKey="CaptureDrop" name="capture drop" stroke="#f2495c" strokeWidth={0} dot={{ r: 4, fill: "#f2495c" }} activeDot={{ r: 5 }} isAnimationActive={false} connectNulls={false} />
            </LineChart>
          </ResponsiveContainer>

          <ChartHeader
            title="Watermark lag"
            legend={[{ color: "#b877d9", label: "lag" }]}
            tooltip="Time between the event watermark and the processing timestamp per cycle. A growing lag means the pipeline is falling behind real-time. Values above 30s are flagged red in the summary cards."
          />
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={chartData} margin={{ top: 4, right: 16, left: 8, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(136,135,128,0.2)" />
              <XAxis dataKey="time" tick={{ fontSize: 11, fill: "#888780" }} interval={Math.max(1, Math.floor(chartData.length / 7))} tickLine={false} axisLine={false} />
              <YAxis tick={{ fontSize: 11, fill: "#888780" }} tickFormatter={(v: number) => fmtMs(v)} tickLine={false} axisLine={false} width={56} />
              <Tooltip content={<WatermarkTooltip />} />
              <Line type="linear" dataKey="WatermarkLagMs" name="watermark lag" stroke="#b877d9" strokeWidth={1.5} dot={{ r: 2, fill: "#b877d9" }} activeDot={{ r: 4 }} isAnimationActive={false} connectNulls={false} />
            </LineChart>
          </ResponsiveContainer>

          {comparison && (
            <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-3">
              <p className="text-xs text-zinc-500 font-medium uppercase tracking-wide mb-3">vs {comparison.period.replace("_", " ")}</p>
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <p className="text-xs text-zinc-400 mb-0.5">avg input</p>
                  <p className="text-sm font-semibold tabular-nums">{Math.round(comparison.avg_input_cardinality).toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-xs text-zinc-400 mb-0.5">avg output</p>
                  <p className="text-sm font-semibold tabular-nums">{Math.round(comparison.avg_output_cardinality).toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-xs text-zinc-400 mb-0.5">drops</p>
                  <p className={`text-sm font-semibold ${comparison.any_drops ? "text-red-500" : "text-zinc-400"}`}>{comparison.any_drops ? "yes" : "none"}</p>
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function EntityValuesPanel({ pipeline, feature, timeWindow, onTraceClick }: {
  pipeline: string; feature: string; timeWindow: string; onTraceClick: (traceId: string) => void;
}) {
  const [data, setData] = useState<EntityValuesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [entityFilter, setEntityFilter] = useState("");

  useEffect(() => {
    if (!pipeline || !feature) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchFeatureValues(feature, { pipeline_id: pipeline, window: timeWindow, entity_id: entityFilter || undefined }, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to load entity values."); })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [pipeline, feature, timeWindow, entityFilter]);

  if (!pipeline || !feature) return <Empty>Select a pipeline and feature above.</Empty>;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2">
        <input
          type="text"
          placeholder="Filter by entity ID…"
          value={entityFilter}
          onChange={(e) => setEntityFilter(e.target.value)}
          className="rounded border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-sm px-2.5 py-1.5 text-zinc-800 dark:text-zinc-100 w-64"
        />
        {entityFilter && (
          <button onClick={() => setEntityFilter("")} className="text-xs text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-300">
            clear
          </button>
        )}
      </div>
      {loading ? <Empty>Loading…</Empty> : error ? <Empty className="text-red-500">{error}</Empty> : !data || data.values.length === 0 ? <Empty>No entity values in this window.</Empty> : (
        <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-zinc-500 border-b border-zinc-100 dark:border-zinc-800">
                <th className="px-4 py-2 font-medium">Entity</th>
                <th className="px-4 py-2 font-medium">Value</th>
                <th className="px-4 py-2 font-medium">Type</th>
                <th className="px-4 py-2 font-medium">Processing time</th>
                <th className="px-4 py-2 font-medium">Trace</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-50 dark:divide-zinc-800">
              {data.values.map((v, i) => {
                const traceId = (v as { trace_id?: string }).trace_id;
                return (
                  <tr key={`${v.entity_id ?? ""}-${v.processing_time}-${i}`} className="hover:bg-zinc-50 dark:hover:bg-zinc-800">
                    <td className="px-4 py-2 font-mono text-xs">{v.entity_id ?? "—"}</td>
                    <td className="px-4 py-2 tabular-nums">{v.feature_value_decoded ?? "—"}</td>
                    <td className="px-4 py-2 text-zinc-500 text-xs">{v.feature_value_type ?? "—"}</td>
                    <td className="px-4 py-2 text-zinc-500 text-xs whitespace-nowrap">{new Date(v.processing_time).toLocaleString()}</td>
                    <td className="px-4 py-2">
                      {traceId
                        ? <button onClick={() => onTraceClick(traceId)} className="text-xs text-blue-500 hover:text-blue-700 font-mono">{traceId.slice(0, 8)}…</button>
                        : <span className="text-zinc-300 dark:text-zinc-700 text-xs">—</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function ValueSummaryPanel({ pipeline, feature, timeWindow }: { pipeline: string; feature: string; timeWindow: string }) {
  const [data, setData] = useState<EntityValueSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!pipeline || !feature) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchFeatureValueSummary(feature, { pipeline_id: pipeline, window: timeWindow }, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to load value summary."); })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [pipeline, feature, timeWindow]);

  if (!pipeline || !feature) return <Empty>Select a pipeline and feature above.</Empty>;
  if (loading) return <Empty>Loading…</Empty>;
  if (error) return <Empty className="text-red-500">{error}</Empty>;
  if (!data) return null;

  const stats = [
    { label: "Entities", value: data.entity_count.toLocaleString() },
    { label: "Min", value: data.value_min?.toLocaleString() ?? "—" },
    { label: "Mean", value: data.value_mean?.toLocaleString() ?? "—" },
    { label: "p50", value: data.value_p50?.toLocaleString() ?? "—" },
    { label: "p95", value: data.value_p95?.toLocaleString() ?? "—" },
    { label: "Max", value: data.value_max?.toLocaleString() ?? "—" },
    { label: "Nulls", value: data.null_count.toLocaleString() },
  ];

  const hasDistribution = data.value_min !== null && data.value_p50 !== null && data.value_p95 !== null && data.value_max !== null;
  const barData = hasDistribution
    ? [{ name: "Distribution", Min: data.value_min!, p50: data.value_p50!, p95: data.value_p95!, Max: data.value_max! }]
    : [];

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-4 gap-3">
        {stats.map(({ label, value }) => <StatCard key={label} label={label} value={value} />)}
      </div>
      {hasDistribution ? (
        <ResponsiveContainer width="100%" height={160}>
          <BarChart data={barData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(136,135,128,0.2)" vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: 11, fill: "#71717a" }} tickLine={false} axisLine={false} />
            <YAxis width={48} tick={{ fontSize: 11, fill: "#71717a" }} tickLine={false} axisLine={false} />
            <Tooltip contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: 6, fontSize: 12 }} labelStyle={{ color: "#a1a1aa" }} itemStyle={{ color: "#e4e4e7" }} />
            <Bar dataKey="Min" fill="#73bf69" radius={[3, 3, 0, 0]} />
            <Bar dataKey="p50" fill="#f2cc0c" radius={[3, 3, 0, 0]} />
            <Bar dataKey="p95" fill="#ff9830" radius={[3, 3, 0, 0]} />
            <Bar dataKey="Max" fill="#f2495c" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      ) : (
        <p className="text-sm text-zinc-500">Distribution not available for non-numeric features.</p>
      )}
    </div>
  );
}

function ViolationsPanel({ pipeline }: { pipeline: string }) {
  const [data, setData] = useState<ViolationsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [severityFilter, setSeverityFilter] = useState("");

  const severityColor: Record<string, string> = {
    CRITICAL: "bg-red-600", HIGH: "bg-orange-500", MEDIUM: "bg-yellow-500", LOW: "bg-zinc-400",
  };

  useEffect(() => {
    if (!pipeline) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchViolations({ pipeline_id: pipeline, severity_gte: severityFilter || undefined }, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to load violations."); })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [pipeline, severityFilter]);

  if (!pipeline) return <Empty>Select a pipeline above.</Empty>;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2">
        <select
          value={severityFilter}
          onChange={(e) => setSeverityFilter(e.target.value)}
          className="rounded border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-sm px-2.5 py-1.5 text-zinc-800 dark:text-zinc-100"
        >
          <option value="">All severities</option>
          <option value="LOW">Low+</option>
          <option value="MEDIUM">Medium+</option>
          <option value="HIGH">High+</option>
          <option value="CRITICAL">Critical only</option>
        </select>
        {data && <span className="text-xs text-zinc-400">{data.total} total</span>}
      </div>
      {loading ? <Empty>Loading…</Empty> : error ? <Empty className="text-red-500">{error}</Empty> : !data || data.violations.length === 0 ? <Empty>No violations found.</Empty> : (
        <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-zinc-500 border-b border-zinc-100 dark:border-zinc-800">
                <th className="px-4 py-2 font-medium">Detected</th>
                <th className="px-4 py-2 font-medium">Feature</th>
                <th className="px-4 py-2 font-medium">Type</th>
                <th className="px-4 py-2 font-medium">Severity</th>
                <th className="px-4 py-2 font-medium">Detail</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-50 dark:divide-zinc-800">
              {data.violations.map((v, i) => (
                <tr key={`${v.detected_at}-${v.violation_type}-${i}`} className="hover:bg-zinc-50 dark:hover:bg-zinc-800">
                  <td className="px-4 py-2 text-xs text-zinc-500 whitespace-nowrap">{new Date(v.detected_at).toLocaleString()}</td>
                  <td className="px-4 py-2 text-xs text-zinc-600 dark:text-zinc-400">{v.feature_name ?? "—"}</td>
                  <td className="px-4 py-2 font-mono text-xs">{v.violation_type}</td>
                  <td className="px-4 py-2">
                    <StatusBadge label={v.severity} active={true} activeColor={severityColor[v.severity] ?? "bg-zinc-400"} />
                  </td>
                  <td className="px-4 py-2 text-xs text-zinc-500 truncate max-w-xs" title={v.detail}>{v.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function OperatorsPanel({ pipeline, timeWindow }: { pipeline: string; timeWindow: string }) {
  const [data, setData] = useState<PipelineHealthResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!pipeline) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchPipelineHealth(pipeline, { window: timeWindow }, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to load pipeline operators."); })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [pipeline, timeWindow]);

  if (!pipeline) return <Empty>Select a pipeline above.</Empty>;
  if (loading) return <Empty>Loading…</Empty>;
  if (error) return <Empty className="text-red-500">{error}</Empty>;
  if (!data || data.operators.length === 0) return <Empty>No operator data for this pipeline.</Empty>;

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs text-zinc-500 border-b border-zinc-100 dark:border-zinc-800">
            <th className="px-4 py-2 font-medium">Operator</th>
            <th className="px-4 py-2 font-medium">Type</th>
            <th className="px-4 py-2 font-medium">Total input</th>
            <th className="px-4 py-2 font-medium">Filter ratio</th>
            <th className="px-4 py-2 font-medium">Last seen</th>
            <th className="px-4 py-2 font-medium">Drops</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-50 dark:divide-zinc-800">
          {data.operators.map((op) => {
            const ratio = op.filter_ratio !== null ? Math.round(op.filter_ratio * 1000) / 10 : null;
            return (
              <tr key={op.operator_id} className="hover:bg-zinc-50 dark:hover:bg-zinc-800">
                <td className="px-4 py-2 font-mono text-xs">{op.operator_id}</td>
                <td className="px-4 py-2 text-xs text-zinc-500">{op.operator_type}</td>
                <td className="px-4 py-2 tabular-nums text-xs">{op.total_input.toLocaleString()}</td>
                <td className="px-4 py-2 text-xs">
                  {ratio !== null ? (
                    <div className="flex items-center gap-2">
                      <div className="w-20 h-1.5 rounded-full bg-zinc-100 dark:bg-zinc-800 overflow-hidden">
                        <div
                          className="h-full rounded-full"
                          style={{
                            width: `${ratio}%`,
                            background: ratio > 80 ? "#1D9E75" : ratio > 50 ? "#ff9830" : "#f2495c",
                          }}
                        />
                      </div>
                      <span className="tabular-nums">{ratio}%</span>
                    </div>
                  ) : "—"}
                </td>
                <td className="px-4 py-2 text-xs text-zinc-500 whitespace-nowrap">{op.last_seen ? new Date(op.last_seen).toLocaleString() : "—"}</td>
                <td className="px-4 py-2">
                  {op.any_drops
                    ? <span className="inline-flex items-center gap-1 text-xs text-red-600 dark:text-red-400 font-medium"><span className="w-1.5 h-1.5 rounded-full bg-red-500 inline-block" />yes</span>
                    : <span className="text-xs text-zinc-400">none</span>}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function EntityLookupPanel({ onFeatureClick }: { onFeatureClick: (feature: string, pipeline: string) => void }) {
  const [entityId, setEntityId] = useState("");
  const [submitted, setSubmitted] = useState("");
  const [data, setData] = useState<EntityFeaturesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  function submit() {
    const id = entityId.trim();
    if (!id) return;
    setSubmitted(id);
    setLoading(true);
    setError(null);
    setData(null);
    const controller = new AbortController();
    fetchEntityFeatures(id, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to look up entity."); })
      .finally(() => setLoading(false));
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center gap-2">
        <input
          ref={inputRef}
          type="text"
          placeholder="Enter entity ID…"
          value={entityId}
          onChange={(e) => setEntityId(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          className="rounded border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-sm px-2.5 py-1.5 text-zinc-800 dark:text-zinc-100 w-72"
        />
        <button
          onClick={submit}
          disabled={!entityId.trim()}
          className="rounded border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-sm px-3 py-1.5 text-zinc-700 dark:text-zinc-200 hover:bg-zinc-50 dark:hover:bg-zinc-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          Look up
        </button>
      </div>

      {loading && <Empty>Looking up {submitted}…</Empty>}
      {error && <Empty className="text-red-500">{error}</Empty>}
      {!loading && data && data.features.length === 0 && <Empty>No features found for entity "{submitted}".</Empty>}
      {!loading && data && data.features.length > 0 && (
        <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
          <div className="px-4 py-2 border-b border-zinc-100 dark:border-zinc-800 flex items-center justify-between">
            <span className="text-xs text-zinc-500">Entity: <span className="font-mono text-zinc-700 dark:text-zinc-300">{data.entity_id}</span></span>
            <span className="text-xs text-zinc-400">{data.features.length} features</span>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-zinc-500 border-b border-zinc-100 dark:border-zinc-800">
                <th className="px-4 py-2 font-medium">Feature</th>
                <th className="px-4 py-2 font-medium">Pipeline</th>
                <th className="px-4 py-2 font-medium">Value</th>
                <th className="px-4 py-2 font-medium">Processing time</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-50 dark:divide-zinc-800">
              {data.features.map((f) => (
                <tr key={`${f.pipeline_id}-${f.feature_name}`} className="hover:bg-zinc-50 dark:hover:bg-zinc-800">
                  <td className="px-4 py-2">
                    <button
                      onClick={() => onFeatureClick(f.feature_name, f.pipeline_id)}
                      className="text-xs text-blue-500 hover:text-blue-700 font-medium"
                    >
                      {f.feature_name}
                    </button>
                  </td>
                  <td className="px-4 py-2 font-mono text-xs text-zinc-500">{f.pipeline_id}</td>
                  <td className="px-4 py-2 tabular-nums text-xs">{f.feature_value_decoded ?? "—"}</td>
                  <td className="px-4 py-2 text-xs text-zinc-500 whitespace-nowrap">{new Date(f.processing_time).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function TraceDrawer({ traceId, onClose }: { traceId: string; onClose: () => void }) {
  const [data, setData] = useState<TraceResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchTrace(traceId, controller.signal)
      .then(setData)
      .catch((err) => { if (err.name !== "AbortError") setError("Failed to load trace."); })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [traceId]);

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-end">
      <div className="absolute inset-0 bg-black/20 dark:bg-black/40" onClick={onClose} />
      <div className="relative w-[600px] h-full bg-white dark:bg-zinc-900 border-l border-zinc-200 dark:border-zinc-800 flex flex-col shadow-xl">
        <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-200 dark:border-zinc-800 shrink-0">
          <div>
            <p className="text-sm font-medium">Trace</p>
            <p className="text-xs font-mono text-zinc-500 mt-0.5">{traceId}</p>
          </div>
          <button onClick={onClose} className="text-zinc-400 hover:text-zinc-700 dark:hover:text-zinc-200">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-4">
          {loading && <Empty>Loading trace…</Empty>}
          {error && <Empty className="text-red-500">{error}</Empty>}
          {data && data.events.length === 0 && <Empty>No events in this trace.</Empty>}
          {data && data.events.length > 0 && (
            <div className="flex flex-col gap-1.5">
              {data.events.map((e) => {
                const filterRatio = e.input_cardinality > 0
                  ? Math.round((e.output_cardinality / e.input_cardinality) * 1000) / 10
                  : null;
                return (
                  <div key={e.span_id} className={`${e.parent_span_id ? "ml-5 border-l-2 border-zinc-200 dark:border-zinc-700 pl-3" : ""} rounded-lg border border-zinc-100 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-800/50 px-3 py-2.5`}>
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-medium text-zinc-700 dark:text-zinc-200">{e.operator_type}</span>
                        <span className="text-xs font-mono text-zinc-400">{e.operator_id}</span>
                      </div>
                      <span className="text-xs text-zinc-400">{fmtTime(e.processing_time)}</span>
                    </div>
                    <div className="flex flex-wrap items-center gap-x-4 gap-y-0.5 text-xs text-zinc-500">
                      {e.feature_name && <span>feature: <span className="text-zinc-700 dark:text-zinc-300">{e.feature_name}</span></span>}
                      <span>in: <span className="tabular-nums text-zinc-700 dark:text-zinc-300">{e.input_cardinality.toLocaleString()}</span></span>
                      <span>out: <span className="tabular-nums text-zinc-700 dark:text-zinc-300">{e.output_cardinality.toLocaleString()}</span></span>
                      {filterRatio !== null && (
                        <span>ratio: <span className={`tabular-nums font-medium ${filterRatio < 50 ? "text-red-500" : filterRatio < 80 ? "text-orange-500" : "text-green-600"}`}>{filterRatio}%</span></span>
                      )}
                      <span className="text-zinc-400 uppercase text-[10px]">{e.capture_mode}</span>
                    </div>
                    <p className="mt-1 text-[10px] font-mono text-zinc-400">
                      span {e.span_id.slice(0, 12)}{e.parent_span_id ? ` · parent ${e.parent_span_id.slice(0, 12)}` : ""}
                    </p>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

const TABS = ["Feature Health", "Entity Values", "Value Summary", "Violations", "Operators", "Entity Lookup"] as const;
type TabName = typeof TABS[number];

const FEATURE_TABS: TabName[] = ["Feature Health", "Entity Values", "Value Summary"];

function DarkToggle() {
  const [dark, setDark] = useState(() =>
    localStorage.getItem("theme") === "dark" || document.documentElement.classList.contains("dark")
  );

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);

  function toggle() {
    const next = !dark;
    setDark(next);
    localStorage.setItem("theme", next ? "dark" : "light");
  }

  return (
    <button onClick={toggle} className="text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100 transition-colors">
      {dark ? (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4">
          <circle cx="12" cy="12" r="5" />
          <line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" />
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
          <line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" />
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
        </svg>
      ) : (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
        </svg>
      )}
    </button>
  );
}

export default function Explore() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [pipelines, setPipelines] = useState<string[]>([]);
  const [pipelineError, setPipelineError] = useState<string | null>(null);
  const [features, setFeatures] = useState<string[]>([]);
  const [featureError, setFeatureError] = useState<string | null>(null);
  const [traceId, setTraceId] = useState<string | null>(null);

  const pipeline = searchParams.get("pipeline") ?? "";
  const feature = searchParams.get("feature") ?? "";
  const timeWindow = searchParams.get("window") ?? "1h";
  const compareTo = searchParams.get("compare") ?? "";
  const rawTab = searchParams.get("tab");
  const activeTab: TabName = (TABS as readonly string[]).includes(rawTab ?? "") ? (rawTab as TabName) : "Feature Health";

  function setParam(key: string, value: string) {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (value) next.set(key, value); else next.delete(key);
      return next;
    }, { replace: true });
  }

  function setPipeline(v: string) {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (v) next.set("pipeline", v); else next.delete("pipeline");
      next.delete("feature");
      return next;
    }, { replace: true });
  }

  useEffect(() => {
    const controller = new AbortController();
    fetchPipelines(controller.signal)
      .then((r) => setPipelines(r.pipelines))
      .catch((err) => { if (err.name !== "AbortError") setPipelineError("Failed to load pipelines."); });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    setFeatures([]);
    setFeatureError(null);
    if (!pipeline) return;
    const controller = new AbortController();
    fetchPipelineFeatures(pipeline, controller.signal)
      .then((r) => setFeatures(r.features))
      .catch((err) => { if (err.name !== "AbortError") setFeatureError("Failed to load features."); });
    return () => controller.abort();
  }, [pipeline]);

  const needsFeature = FEATURE_TABS.includes(activeTab as typeof FEATURE_TABS[number]);

  function handleFeatureClick(feat: string, pipe: string) {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.set("pipeline", pipe);
      next.set("feature", feat);
      next.set("tab", "Feature Health");
      return next;
    }, { replace: true });
  }

  return (
    <div className="h-full flex flex-col bg-zinc-50 dark:bg-zinc-950">
      {/* query bar */}
      <div className="border-b border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2.5">
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-semibold text-sm tracking-tight mr-2">🔭 Faro</span>
          <div className="w-px h-4 bg-zinc-200 dark:bg-zinc-700 shrink-0" />
          {pipelineError ? (
            <span className="text-xs text-red-500">{pipelineError}</span>
          ) : (
            <select
              value={pipeline}
              onChange={(e) => setPipeline(e.target.value)}
              className="rounded border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-sm px-2.5 py-1.5 text-zinc-800 dark:text-zinc-100 min-w-40"
            >
              <option value="">Pipeline</option>
              {pipelines.map((p) => <option key={p} value={p}>{p}</option>)}
            </select>
          )}
          {needsFeature && (
            <>
              <svg className="w-3.5 h-3.5 text-zinc-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
              {featureError ? (
                <span className="text-xs text-red-500">{featureError}</span>
              ) : (
                <select
                  value={feature}
                  onChange={(e) => setParam("feature", e.target.value)}
                  disabled={!pipeline || features.length === 0}
                  className="rounded border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-sm px-2.5 py-1.5 text-zinc-800 dark:text-zinc-100 min-w-40 disabled:opacity-40"
                >
                  <option value="">Feature</option>
                  {features.map((f) => <option key={f} value={f}>{f}</option>)}
                </select>
              )}
            </>
          )}
          <div className="ml-auto flex items-center gap-2">
            <WindowSelector value={timeWindow} onChange={(v) => setParam("window", v)} />
            <DarkToggle />
          </div>
        </div>
      </div>

      {/* tab bar */}
      <div className="border-b border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4">
        <div className="flex gap-0">
          {TABS.map((tab) => (
            <button
              key={tab}
              onClick={() => setParam("tab", tab)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab
                  ? "border-blue-500 text-blue-600 dark:text-blue-400"
                  : "border-transparent text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200 hover:border-zinc-300 dark:hover:border-zinc-600"
              }`}
            >
              {tab}
            </button>
          ))}
        </div>
      </div>

      {/* panel */}
      <div className="flex-1 overflow-y-auto p-6">
        {activeTab === "Feature Health" && (
          <FeatureHealthPanel pipeline={pipeline} feature={feature} timeWindow={timeWindow} compareTo={compareTo} onCompareChange={(v) => setParam("compare", v)} />
        )}
        {activeTab === "Entity Values" && (
          <EntityValuesPanel pipeline={pipeline} feature={feature} timeWindow={timeWindow} onTraceClick={setTraceId} />
        )}
        {activeTab === "Value Summary" && (
          <ValueSummaryPanel pipeline={pipeline} feature={feature} timeWindow={timeWindow} />
        )}
        {activeTab === "Violations" && (
          <ViolationsPanel pipeline={pipeline} />
        )}
        {activeTab === "Operators" && (
          <OperatorsPanel pipeline={pipeline} timeWindow={timeWindow} />
        )}
        {activeTab === "Entity Lookup" && (
          <EntityLookupPanel onFeatureClick={handleFeatureClick} />
        )}
      </div>

      {traceId && <TraceDrawer traceId={traceId} onClose={() => setTraceId(null)} />}
    </div>
  );
}
