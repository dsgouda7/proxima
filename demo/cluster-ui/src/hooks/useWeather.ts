import { useEffect, useState } from 'react';

export interface WeatherMetrics {
  source:     string;
  trie_size:  number;
  last_sync:  number | null;
  metrics:    { write_count: number; read_count: number };
}

export interface StationEvent {
  n:         number;
  total:     number;
  id:        string;
  lat:       number;
  lon:       number;
  temp_c:    number;
  condition: string;
  wmo_code:  number;
  complete:  boolean;
}

const WMO_EMOJI: Record<number, string> = {
  0:'☀️',1:'🌤️',2:'⛅',3:'☁️',
  45:'🌫️',48:'🌫️',51:'🌦️',53:'🌦️',55:'🌧️',
  61:'🌧️',63:'🌧️',65:'🌧️',66:'🌨️',67:'🌨️',
  71:'❄️',73:'❄️',75:'❄️',77:'🌨️',
  80:'🌦️',81:'🌦️',82:'🌧️',85:'🌨️',86:'🌨️',
  95:'⛈️',96:'⛈️',99:'⛈️',
};
export const getWmoEmoji = (code: number) => WMO_EMOJI[code] ?? '🌡️';

export function useWeather(weatherUrl = 'http://localhost:3001') {
  const [metrics,   setMetrics]   = useState<WeatherMetrics | null>(null);
  const [events,    setEvents]    = useState<StationEvent[]>([]);
  const [streaming, setStreaming] = useState<{ n: number; total: number } | null>(null);
  const [reachable, setReachable] = useState(false);

  // Poll /api/metrics every 3 s
  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const m = await fetch(`${weatherUrl}/api/metrics`, { signal: AbortSignal.timeout(2000) })
          .then(r => r.json()) as WeatherMetrics;
        if (!cancelled) { setMetrics(m); setReachable(true); }
      } catch {
        if (!cancelled) setReachable(false);
      }
    };
    poll();
    const id = setInterval(poll, 3000);
    return () => { cancelled = true; clearInterval(id); };
  }, [weatherUrl]);

  // SSE subscription to /api/stream
  useEffect(() => {
    const es = new EventSource(`${weatherUrl}/api/stream`);

    es.addEventListener('station', (e: MessageEvent) => {
      const data: StationEvent = JSON.parse(e.data);
      if (data.complete) {
        setStreaming(null);
      } else {
        setStreaming({ n: data.n + 1, total: data.total });
      }
      setEvents(prev => [...prev.slice(-11), data]);
    });

    es.addEventListener('keepalive', () => {});
    es.onerror = () => es.close();
    return () => es.close();
  }, [weatherUrl]);

  return { metrics, events, streaming, reachable };
}
