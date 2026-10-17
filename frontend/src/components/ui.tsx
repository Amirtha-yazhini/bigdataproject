import { ReactNode } from "react";
import { Level, LEVEL_COLOR } from "../api";

export function Card({ title, subtitle, right, children, className = "" }:
  { title?: ReactNode; subtitle?: ReactNode; right?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`card p-4 min-w-0 ${className}`}>
      {(title || right) && (
        <header className="flex items-start justify-between gap-3 mb-3">
          <div className="min-w-0">
            {title && <h2 className="text-sm font-semibold text-ink">{title}</h2>}
            {subtitle && <p className="text-xs text-muted mt-0.5">{subtitle}</p>}
          </div>
          {right}
        </header>
      )}
      {children}
    </section>
  );
}

export function Kpi({ label, value, unit, note, tone }: { label: string; value: ReactNode; unit?: string; note?: ReactNode; tone?: Level }) {
  return (
    <div className="card px-4 py-3 min-w-0">
      <div className="text-xs text-ink2 flex items-center gap-1.5">
        {tone && <LevelIcon level={tone} size={10} />}
        {label}
      </div>
      <div className="mt-1 text-2xl font-semibold text-ink">
        {value}
        {unit && <span className="text-sm font-normal text-ink2 ml-1">{unit}</span>}
      </div>
      {note && <div className="text-xs text-muted mt-0.5">{note}</div>}
    </div>
  );
}

/** status never relies on color alone: each level has its own glyph + text label */
export function LevelIcon({ level, size = 12 }: { level: Level; size?: number }) {
  const c = LEVEL_COLOR[level];
  const s = size;
  return (
    <svg width={s} height={s} viewBox="0 0 12 12" aria-hidden className="shrink-0">
      {level === "LOW" && <circle cx="6" cy="6" r="5" fill={c} />}
      {level === "MODERATE" && <rect x="1" y="1" width="10" height="10" rx="2" fill={c} />}
      {level === "HIGH" && <path d="M6 0.5 11.5 11H0.5Z" fill={c} />}
      {level === "CRITICAL" && <path d="M6 0.5 11.5 6 6 11.5 0.5 6Z" fill={c} />}
      {level === "UNKNOWN" && <circle cx="6" cy="6" r="4.5" fill="none" stroke={c} strokeWidth="1.5" />}
    </svg>
  );
}

export function LevelBadge({ level, label }: { level: Level; label?: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-ink2 whitespace-nowrap">
      <LevelIcon level={level} />
      {label ?? level.charAt(0) + level.slice(1).toLowerCase()}
    </span>
  );
}

export function RouteBullet({ id, color, textColor = "#fff", size = 20 }: { id: string; color: string; textColor?: string; size?: number }) {
  return (
    <span className="inline-flex items-center justify-center rounded-full font-bold shrink-0"
      style={{ background: color, color: textColor, width: size, height: size, fontSize: size * 0.55 }}>
      {id}
    </span>
  );
}

export function ChartTooltip({ active, payload, label, labelFormatter, valueFormatter }: {
  active?: boolean; payload?: { name: string; value: number; color: string; dataKey: string }[]; label?: number | string;
  labelFormatter?: (l: number | string) => string; valueFormatter?: (v: number, key: string) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="card px-3 py-2 text-xs shadow-lg">
      <div className="text-ink2 mb-1">{labelFormatter && label != null ? labelFormatter(label) : label}</div>
      {payload.filter((p) => p.value != null).map((p) => (
        <div key={p.dataKey} className="flex items-center gap-2">
          <span className="inline-block w-2.5 h-0.5 rounded" style={{ background: p.color, height: 3 }} />
          <span className="text-ink2">{p.name}</span>
          <span className="ml-auto pl-3 font-medium text-ink tnum">{valueFormatter ? valueFormatter(p.value, p.dataKey) : p.value}</span>
        </div>
      ))}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="text-sm text-muted py-8 text-center">{children}</div>;
}

export const axisProps = { stroke: "var(--axis)", tickLine: false, tick: { fill: "var(--muted)", fontSize: 11 } };
