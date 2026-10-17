import { NavLink, Route, Routes } from "react-router-dom";
import { fmtDay, fmtTime } from "./api";
import { useLive } from "./hooks/useLive";
import Forecast from "./pages/Forecast";
import Incidents from "./pages/Incidents";
import LiveMap from "./pages/LiveMap";
import Recommendations from "./pages/Recommendations";
import System from "./pages/System";

const NAV = [
  ["/", "Live map"], ["/forecast", "Crowding forecast"], ["/recommendations", "Recommendations"],
  ["/incidents", "Incidents & impact"], ["/system", "Pipeline & data"],
] as const;

export default function App() {
  const { live, network, connected, error } = useLive();
  const pending = live?.kpis.pending_recommendations ?? 0;
  const openInc = live?.kpis.open_incidents ?? 0;
  return (
    <div className="min-h-full flex flex-col">
      <header className="border-b border-line bg-surface sticky top-0 z-[1000]">
        <div className="max-w-[1600px] mx-auto px-4 py-2 flex flex-wrap items-center gap-x-6 gap-y-2">
          <div className="flex items-center gap-2">
            <svg width="22" height="22" viewBox="0 0 24 24" aria-hidden><rect x="3" y="3" width="18" height="14" rx="3" fill="var(--accent)" /><circle cx="8" cy="20" r="2" fill="var(--accent)" /><circle cx="16" cy="20" r="2" fill="var(--accent)" /></svg>
            <div>
              <div className="text-sm font-semibold leading-tight">Transit Flow Optimizer</div>
              <div className="text-[11px] text-muted leading-tight">{network?.city ?? "loading network…"}</div>
            </div>
          </div>
          <nav className="flex flex-wrap gap-1 text-sm">
            {NAV.map(([to, label]) => (
              <NavLink key={to} to={to} end={to === "/"}
                className={({ isActive }) => `px-3 py-1.5 rounded-md ${isActive ? "bg-raised text-ink font-medium" : "text-ink2 hover:bg-raised"}`}>
                {label}
                {to === "/recommendations" && pending > 0 && <span className="ml-1.5 text-[11px] px-1.5 rounded-full bg-accent text-white tnum">{pending}</span>}
                {to === "/incidents" && openInc > 0 && <span className="ml-1.5 text-[11px] px-1.5 rounded-full bg-critical text-white tnum">{openInc}</span>}
              </NavLink>
            ))}
          </nav>
          <div className="ml-auto flex items-center gap-4 text-xs text-ink2">
            {network && (
              <span className="px-2 py-0.5 rounded border border-line" title="Which data sources are real vs. synthetic: see Pipeline & data">
                Data: {network.mode}
              </span>
            )}
            <span className="tnum">Sim time <b className="text-ink">{fmtDay(live?.clock)} {fmtTime(live?.clock, true)}</b></span>
            <span className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full" style={{ background: connected ? "var(--good)" : "var(--warning)" }} />
              {connected ? "Live" : "Reconnecting"}
            </span>
          </div>
        </div>
        {error && <div className="text-xs text-center py-1 bg-raised text-ink2">Backend: {error}</div>}
      </header>
      <main className="flex-1 max-w-[1600px] w-full mx-auto p-4">
        <Routes>
          <Route path="/" element={<LiveMap />} />
          <Route path="/forecast" element={<Forecast />} />
          <Route path="/recommendations" element={<Recommendations />} />
          <Route path="/incidents" element={<Incidents />} />
          <Route path="/system" element={<System />} />
        </Routes>
      </main>
    </div>
  );
}
