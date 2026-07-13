import { useState, useEffect, useCallback, useRef } from 'react';
import { MapContainer, TileLayer, useMapEvents } from 'react-leaflet';
import type { Map as LeafletMap } from 'leaflet';
import { fetchAllAircraft, fetchRegion, fetchMetrics, fetchAircraftDetail } from './api/client';
import { Aircraft, MetricsResponse } from './types';
import AircraftMarker from './components/AircraftMarker';
import AircraftPanel from './components/AircraftPanel';
import MetricsPanel from './components/MetricsPanel';
import TrieExplorer from './components/TrieExplorer';
import 'leaflet/dist/leaflet.css';
const REGION_ZOOM = 6;   // switch to Redis region query above this zoom
const DETAIL_ZOOM = 9;   // fetch SQLite detail below this count
const DETAIL_MAX  = 5;   // only fetch detail when <= this many aircraft in view
const WEATHER_STATION_ZOOM = 10; // server returns individual weather stations here

// Prevent querying ghost world-copies when the user pans far east/west.
function clampBounds(s: number, w: number, n: number, e: number) {
  if (e - w >= 358) return { s: -85, w: -180, n: 85, e: 180 }; // whole world
  const cw = Math.max(-180, Math.min(180, w));
  const ce = Math.max(-180, Math.min(180, e));
  return { s, w: cw, n, e: ce };
}

function MapWatcher({ onBounds }: { onBounds: (s:number,w:number,n:number,e:number,z:number)=>void }) {
  const map = useMapEvents({
    moveend: () => fire(map, onBounds),
    zoomend: () => fire(map, onBounds),
  });
  useEffect(() => {
    fire(map, onBounds);
    const id = setInterval(() => fire(map, onBounds), 60_000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return null;
}

function fire(map: LeafletMap, cb: (s:number,w:number,n:number,e:number,z:number)=>void) {
  const b = map.getBounds();
  cb(b.getSouth(), b.getWest(), b.getNorth(), b.getEast(), map.getZoom());
}

export default function App() {
  const [aircraft, setAircraft] = useState<Aircraft[]>([]);
  const [metrics,  setMetrics]  = useState<MetricsResponse | null>(null);
  const [status,   setStatus]   = useState('Loading live data...');
  const [isWeather, setIsWeather] = useState(false);
  const [streamProgress, setStreamProgress] = useState<{n:number,total:number}|null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const mapRef        = useRef<LeafletMap | null>(null);
  const lastBoundsRef = useRef<[number,number,number,number,number] | null>(null);

  const handleBounds = useCallback(async (s:number, w:number, n:number, e:number, zoom:number) => {
    lastBoundsRef.current = [s, w, n, e, zoom];
    try {
      const { s: cs, w: cw, n: cn, e: ce } = clampBounds(s, w, n, e);
      let res;
      if (zoom >= REGION_ZOOM) {
        res = await fetchRegion(cs, cw, cn, ce, zoom);
      } else {
        res = await fetchAllAircraft(zoom);
      }
      // A deep-zoom viewport may contain no stations. Preserve the previously
      // established mode in that case instead of relabelling a weather map as
      // an aircraft tracker merely because the response is empty.
      const responseIsWeather = res.aircraft.some(a => a.payload.__is_weather === true);
      const weather = res.aircraft.length > 0 ? responseIsWeather : isWeather;
      if (res.aircraft.length > 0) setIsWeather(responseIsWeather);
      const subject = weather ? 'weather stations' : 'aircraft';
      setStatus(zoom >= REGION_ZOOM
        ? `${res.count.toLocaleString()} ${subject} · S2 region · zoom ${zoom} · ${new Date().toLocaleTimeString()}`
        : `${res.count.toLocaleString()} ${subject} worldwide · zoom in for regional detail`);

      // On deep zoom with few aircraft: hydrate from SQLite for history + full metadata
      if (zoom >= DETAIL_ZOOM && res.aircraft.length > 0 && res.aircraft.length <= DETAIL_MAX) {
        setStatus(prev => prev + ' · fetching details...');
        const enriched = await Promise.all(
          res.aircraft.map(async a => {
            const detail = await fetchAircraftDetail(a.id);
            if (!detail) return a;
            return {
              ...a,
              payload: {
                ...a.payload,
                callsign:       detail.callsign ?? a.payload.callsign,
                origin_country: detail.origin_country || a.payload.origin_country,
                altitude:       detail.altitude  ?? a.payload.altitude,
                velocity:       detail.velocity  ?? a.payload.velocity,
                heading:        detail.heading   ?? a.payload.heading,
                on_ground:      detail.on_ground,
                history:        detail.history,  // trail from SQLite
              },
            };
          })
        );
        setAircraft(enriched);
        setStatus(`${enriched.length} ${subject} · full detail (SQLite) · zoom ${zoom}`);
      } else {
        setAircraft(res.aircraft);
      }
    } catch {
      setStatus('API unreachable');
    }
  }, []);

  const handleSelect = useCallback((a: Aircraft) => {
    setSelected(a.id);
    const isWeatherCluster = a.payload.__is_weather === true && (a.payload.count ?? 1) > 1;
    const targetZoom = isWeatherCluster ? WEATHER_STATION_ZOOM : DETAIL_ZOOM;
    mapRef.current?.flyTo([a.lat, a.lon], Math.max(mapRef.current.getZoom(), targetZoom), { animate: true, duration: 0.8 });
  }, []);

  const handleHover = useCallback((a: Aircraft) => setHoveredId(a.id), []);
  const handleHoverEnd = useCallback(() => setHoveredId(null), []);

  // Metrics poller (every 10s)
  useEffect(() => {
    const poll = async () => { try { setMetrics(await fetchMetrics()); } catch { /**/ } };
    void poll();
    const id = setInterval(poll, 10_000);
    return () => clearInterval(id);
  }, []);

  // SSE subscription — weather server streams one StationEvent per METAR insertion.
  // Aircraft server has no /api/stream so EventSource closes cleanly.
  useEffect(() => {
    const es = new EventSource('/api/stream');

    // Rich per-station event from the weather server
    es.addEventListener('station', (e) => {
      const data = JSON.parse(e.data) as {n:number; total:number; complete:boolean};
      setStreamProgress({ n: data.n + 1, total: data.total });
      // Refresh map every 200 stations during streaming, and on completion
      if (data.complete || data.n % 200 === 199) {
        if (lastBoundsRef.current) void handleBounds(...lastBoundsRef.current);
        if (data.complete) {
          setTimeout(() => setStreamProgress(null), 3000);
        }
      }
    });

    // Legacy simple update event (fallback for future servers)
    es.addEventListener('update', () => {
      if (lastBoundsRef.current) void handleBounds(...lastBoundsRef.current);
    });

    es.onerror = () => es.close();
    return () => es.close();
  }, [handleBounds]);

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', background: '#020617' }}>
      <header style={{ padding: '7px 16px', background: 'linear-gradient(90deg,#0c1528,#0f172a)', borderBottom: '1px solid rgba(56,189,248,0.15)', display: 'flex', alignItems: 'center', gap: 12, flexShrink: 0, boxShadow: '0 2px 12px rgba(0,0,0,0.5)' }}>
        <span style={{ fontSize: 20 }}>{isWeather ? '🌦️' : '✈'}</span>
        <strong style={{ fontSize: '1rem', color: '#38bdf8', letterSpacing: 0.5 }}>proxima</strong>
        <span style={{ color: '#475569', fontSize: 12 }}>·</span>
        <span style={{ color: '#94a3b8', fontSize: 12 }}>{isWeather ? 'Live Weather Stations' : 'Live Aircraft Tracker'}</span>
        <span style={{ marginLeft: 'auto', fontSize: '0.72rem', color: '#64748b', maxWidth: 500, textAlign: 'right', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{status}</span>
        {streamProgress && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
            <div style={{ fontSize: '0.7rem', color: '#38bdf8', whiteSpace: 'nowrap' }}>
              ⚡ Streaming {streamProgress.n.toLocaleString()} / {streamProgress.total.toLocaleString()}
            </div>
            <div style={{ width: 80, height: 4, background: '#1e293b', borderRadius: 2, overflow: 'hidden' }}>
              <div style={{
                height: '100%', borderRadius: 2,
                background: 'linear-gradient(90deg,#38bdf8,#818cf8)',
                width: `${(streamProgress.n / streamProgress.total * 100).toFixed(0)}%`,
                transition: 'width 0.1s ease',
              }} />
            </div>
          </div>
        )}
      </header>
      <div style={{ flex: 1, position: 'relative' }}>
        <MapContainer
          center={[20, 0]}
          zoom={3}
          style={{ height: '100%', width: '100%', background: '#0c1a2e' }}
          ref={mapRef}
          minZoom={-1}
          worldCopyJump={true}
        >
          <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" attribution='&copy; OSM &copy; CARTO' minZoom={0} maxZoom={19} />
          <MapWatcher onBounds={handleBounds} />
          {aircraft.map(a => (
            <AircraftMarker
              key={a.id} aircraft={a} onClick={handleSelect}
              onHover={handleHover} onHoverEnd={handleHoverEnd}
              simplified={!isWeather && aircraft.length > 100}
            />
          ))}
        </MapContainer>
        <AircraftPanel aircraft={aircraft} onSelect={handleSelect} selected={selected} isWeather={isWeather} />
        {metrics && <MetricsPanel metrics={metrics} entityLabel={isWeather ? 'Stations' : 'Aircraft'} />}
        <div style={{ position: 'absolute', bottom: 24, left: 10, zIndex: 1000, display: 'flex', alignItems: 'flex-end', gap: 10 }}>
          <div style={{ background: 'rgba(15,23,42,0.9)', borderRadius: 8, padding: '8px 12px', fontSize: 10, color: '#94a3b8', border: '1px solid rgba(255,255,255,0.07)', backdropFilter: 'blur(4px)' }}>
            <div style={{ fontWeight: 700, marginBottom: 5, color: '#e2e8f0', fontSize: 11 }}>{isWeather ? 'Temperature' : 'Altitude'}</div>
            {((isWeather
              ? [['#ef4444','≥35°C'],['#f97316','25–35°C'],['#eab308','15–25°C'],['#22c55e','5–15°C'],['#06b6d4','−5–5°C'],['#3b82f6','<−5°C']]
              : [['#a78bfa','>10 km'],['#38bdf8','7-10 km'],['#34d399','3-7 km'],['#fbbf24','0.5-3 km'],['#f87171','<500 m'],['#64748b','Ground']]
            ) as [string,string][]).map(([c,l]) => (
              <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
                <div style={{ width: 10, height: 10, borderRadius: '50%', background: c, flexShrink: 0 }} />
                <span>{l}</span>
              </div>
            ))}
            <div style={{ marginTop: 8, fontSize: 9, color: '#334155', borderTop: '1px solid rgba(255,255,255,0.05)', paddingTop: 6 }}>
              {isWeather ? 'Zoom 10+ · individual METAR stations' : 'Zoom 9+ · <5 aircraft: full detail from SQLite'}
            </div>
          </div>
          <TrieExplorer highlightId={hoveredId ?? selected} />
        </div>
      </div>
    </div>
  );
}
