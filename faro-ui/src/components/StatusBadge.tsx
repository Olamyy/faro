interface StatusBadgeProps {
  label: string;
  active: boolean;
  activeColor?: string;
}

export function StatusBadge({ label, active, activeColor = "bg-red-500" }: StatusBadgeProps) {
  if (!active) return null;
  return (
    <span className={`inline-flex items-center rounded px-2 py-0.5 text-xs font-medium text-white ${activeColor}`}>
      {label}
    </span>
  );
}
