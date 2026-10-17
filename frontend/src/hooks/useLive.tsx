import { createContext, ReactNode, useContext, useEffect, useRef, useState } from "react";
import { api, Live, Network } from "../api";

interface Ctx { live: Live | null; network: Network | null; connected: boolean; error: string | null; refresh: () => void }
const LiveContext = createContext<Ctx>({ live: null, network: null, connected: false, error: null, refresh: () => {} });

/** One WebSocket for the whole app (server pushes every 2 s); falls back to polling /api/live. */
export function LiveProvider({ children }: { children: ReactNode }) {
  const [live, setLive] = useState<Live | null>(null);
  const [network, setNetwork] = useState<Network | null>(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  const refresh = () => api<Live>("/api/live").then(setLive).catch((e) => setError(String(e)));

  useEffect(() => {
    api<Network>("/api/network").then(setNetwork).catch((e) => setError(String(e)));
    let closed = false;
    let poll: number | undefined;
    let retry: number | undefined;
    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${proto}://${location.host}/ws/live`);
      wsRef.current = ws;
      ws.onopen = () => { setConnected(true); setError(null); window.clearInterval(poll); };
      ws.onmessage = (m) => {
        const data = JSON.parse(m.data);
        if (data.error) setError(data.error); else setLive(data);
      };
      ws.onclose = () => {
        setConnected(false);
        if (closed) return;
        window.clearInterval(poll);
        poll = window.setInterval(refresh, 3000);
        retry = window.setTimeout(connect, 5000);
      };
    };
    refresh();
    connect();
    return () => { closed = true; window.clearInterval(poll); window.clearTimeout(retry); wsRef.current?.close(); };
  }, []);

  return <LiveContext.Provider value={{ live, network, connected, error, refresh }}>{children}</LiveContext.Provider>;
}

export const useLive = () => useContext(LiveContext);

export function usePoll<T>(path: string | null, ms = 5000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!path) { setData(null); return; }
    let alive = true;
    const load = () => api<T>(path).then((d) => alive && (setData(d), setError(null))).catch((e) => alive && setError(String(e)));
    load();
    const t = window.setInterval(load, ms);
    return () => { alive = false; window.clearInterval(t); };
  }, [path, ms]);
  return { data, error };
}
