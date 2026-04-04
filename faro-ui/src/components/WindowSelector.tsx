import { useState } from "react";

const PRESETS = ["15m", "1h", "6h", "24h", "7d"] as const;
const WINDOW_RE = /^\d+[hmd]$/;

interface WindowSelectorProps {
  value: string;
  onChange: (value: string) => void;
}

export function WindowSelector({ value, onChange }: WindowSelectorProps) {
  const [custom, setCustom] = useState("");
  const [error, setError] = useState("");
  const isCustom = !PRESETS.includes(value as (typeof PRESETS)[number]);

  function handlePreset(v: string) {
    setError("");
    setCustom("");
    onChange(v);
  }

  function handleCustomChange(v: string) {
    setCustom(v);
    if (v === "") {
      setError("");
      return;
    }
    if (!WINDOW_RE.test(v)) {
      setError("Use format: 1h, 30m, 7d");
      return;
    }
    setError("");
    onChange(v);
  }

  return (
    <div className="flex items-center gap-1.5">
      <div className="flex rounded border border-zinc-300 dark:border-zinc-600 overflow-hidden bg-white dark:bg-zinc-800">
        {PRESETS.map((p) => (
          <button
            key={p}
            onClick={() => handlePreset(p)}
            className={`px-2.5 py-1 text-xs font-medium transition-colors border-r border-zinc-300 dark:border-zinc-600 last:border-r-0 ${
              value === p
                ? "bg-blue-500 text-white"
                : "text-zinc-600 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-700"
            }`}
          >
            {p}
          </button>
        ))}
      </div>
      <div className="relative">
        <input
          type="text"
          placeholder="custom"
          value={isCustom ? value : custom}
          onChange={(e) => handleCustomChange(e.target.value)}
          className={`w-16 rounded border px-2 py-1 text-xs dark:bg-zinc-800 dark:text-zinc-100 ${
            error
              ? "border-red-400 focus:outline-none focus:ring-1 focus:ring-red-400"
              : "border-zinc-300 dark:border-zinc-600"
          }`}
        />
        {error && (
          <span className="absolute top-full left-0 mt-0.5 text-xs text-red-500 whitespace-nowrap bg-white dark:bg-zinc-900 px-1 rounded shadow z-10">
            {error}
          </span>
        )}
      </div>
    </div>
  );
}
