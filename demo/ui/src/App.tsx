import { useState, useEffect, useCallback, useRef } from 'react';
import { MapContainer, TileLayer, useMapEvents } from 'react-leaflet';
import type { Map as LeafletMap } from 'leaflet';
import { fetchAllAircraft, fetchRegion, fetchMetrics, fetchAircraftDetail } from './api/client';
import { Aircraft, MetricsResponse } from './types';
import AircraftMarker from './components/AircraftMarker';
import AircraftPanel from './components/AircraftPanel';
import MetricsPanel from './components/MetricsPanel';
import 'leaflet/dist/leaflet.css';

const REGION_ZOOM = 6;   // switch to Redis region query above this zoom
const DETAIL_ZOOM = 9;   // fetch SQLite detail below this count
const DETAIL_MAX  = 5;   // only fetch detail when <= this many aircraft in view

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
  const [status,   setStatus]   = useState('Loading live aircraft...');
  const [selected, setSelected] = useState<string | null>(null);
  const mapRef = useRef<LeafletMap | null>(null);

  const handleBounds = useCallback(async (s:number, w:number, n:number, e:number, zoom:number) => {
    try {
      const { s: cs, w: cw, n: cn, e: ce } = clampBounds(s, w, n, e);
      let res;
      if (zoom >= REGION_ZOOM) {
        res = await fetchRegion(cs, cw, cn, ce);
        setStatus(`${res.count.toLocaleString()} aircraft · Redis S2 region · zoom ${zoom} · ${new Date().toLocaleTimeString()}`);
      } else {
        res = await fetchAllAircraft();
        setStatus(`${res.count.toLocaleString()} aircraft worldwide · zoom in for region query`);
      }

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
        setStatus(`${enriched.length} aircraft · full detail (SQLite) · zoom ${zoom}`);
      } else {
        setAircraft(res.aircraft);
      }
    } catch {
      setStatus('API unreachable');
    }
  }, []);

  const handleSelect = useCallback((a: Aircraft) => {
    setSelected(a.id);
    mapRef.current?.flyTo([a.lat, a.lon], Math.max(mapRef.current.getZoom(), 9), { animate: true, duration: 0.8 });
  }, []);

  useEffect(() => {
    const poll = async () => { try { setMetrics(await fetchMetrics()); } catch { /**/ } };
    void poll();
    const id = setInterval(poll, 10_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', background: '#020617' }}>
      <header style={{ padding: '7px 16px', background: 'linear-gradient(90deg,#0c1528,#0f172a)', borderBottom: '1px solid rgba(56,189,248,0.15)', display: 'flex', alignItems: 'center', gap: 12, flexShrink: 0, boxShadow: '0 2px 12px rgba(0,0,0,0.5)' }}>
        <span style={{ fontSize: 20 }}>✈</span>
        <strong style={{ fontSize: '1rem', color: '#38bdf8', letterSpacing: 0.5 }}>GeoRedis</strong>
        <span style={{ color: '#475569', fontSize: 12 }}>·</span>
        <span style={{ color: '#94a3b8', fontSize: 12 }}>Live Aircraft Tracker</span>
        <span style={{ marginLeft: 'auto', fontSize: '0.72rem', color: '#64748b', maxWidth: 500, textAlign: 'right', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{status}</span>
      </header>
      <div style={{ flex: 1, position: 'relative' }}>
        <MapContainer
          center={[20, 0]}
          zoom={3}
          style={{ height: '100%', width: '100%', background: '#0c1a2e' }}
          ref={mapRef}
          worldCopyJump={true}
        >
          <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" attribution='&copy; OSM &copy; CARTO' maxZoom={19} />
          <MapWatcher onBounds={handleBounds} />
          {aircraft.map(a => <AircraftMarker key={a.id} aircraft={a} onClick={handleSelect} />)}
        </MapContainer>
        <AircraftPanel aircraft={aircraft} onSelect={handleSelect} selected={selected} />
        {metrics && <MetricsPanel metrics={metrics} />}
        <div style={{ position: 'absolute', bottom: 24, left: 10, zIndex: 1000, background: 'rgba(15,23,42,0.9)', borderRadius: 8, padding: '8px 12px', fontSize: 10, color: '#94a3b8', border: '1px solid rgba(255,255,255,0.07)', backdropFilter: 'blur(4px)' }}>
          <div style={{ fontWeight: 700, marginBottom: 5, color: '#e2e8f0', fontSize: 11 }}>Altitude</div>
          {([['#a78bfa','>10 km'],['#38bdf8','7-10 km'],['#34d399','3-7 km'],['#fbbf24','0.5-3 km'],['#f87171','<500 m'],['#64748b','Ground']] as [string,string][]).map(([c,l]) => (
            <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
              <div style={{ width: 10, height: 10, borderRadius: '50%', background: c, flexShrink: 0 }} />
              <span>{l}</span>
            </div>
          ))}
          <div style={{ marginTop: 8, fontSize: 9, color: '#334155', borderTop: '1px solid rgba(255,255,255,0.05)', paddingTop: 6 }}>
            Zoom 9+ · &lt;5 aircraft: full detail from SQLite
          </div>
        </div>
      </div>
    </div>
  );
}
