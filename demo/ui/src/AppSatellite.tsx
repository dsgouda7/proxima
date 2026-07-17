import { useState, useEffect, useCallback, useRef, Fragment } from 'react';
import { MapContainer, TileLayer, CircleMarker, Tooltip, Polygon, ZoomControl, useMapEvents } from 'react-leaflet';
import type { Map as LeafletMap } from 'leaflet';
import 'leaflet/dist/leaflet.css';

// Longitude offsets for the visible wrapped world copies, so vector/marker
// layers repeat horizontally like the tile layer does.
const WORLD_COPIES = [-360, 0, 360];

interface Satellite { id: number; name: string; lat: number; lon: number; altitude: number; launchDate: string | null; category: string; }
interface SatelliteMetrics { totalCount: number; lastUpdate: string; categories: Array<{ category: string; count: number }>; }
interface TerminatorData { coordinates: Array<{ lat: number; lon: number }>; nightSide: string; }

async function fetchAllSatellites(): Promise<Satellite[]> {
  const res = await fetch('/api/satellites');
  const data = await res.json();
  return data.satellites || [];
}

async function fetchMetrics(): Promise<SatelliteMetrics> {
  return await (await fetch('/api/metrics')).json();
}

async function fetchTerminator(): Promise<TerminatorData> {
  return await (await fetch('/api/terminator')).json();
}

function getCategoryColor(category: string): string {
  switch (category) {
    case 'space-station': return '#FF8C00';
    case 'communication': return '#0066CC';
    case 'navigation': return '#DC143C';
    case 'weather': return '#228B22';
    case 'earth-observation': return '#8B008B';
    default: return '#666666';
  }
}

function getCategoryLabel(category: string): string {
  switch (category) {
    case 'space-station': return 'Space Station';
    case 'communication': return 'Communication';
    case 'navigation': return 'Navigation (GPS)';
    case 'weather': return 'Weather';
    case 'earth-observation': return 'Earth Observation';
    default: return 'Other';
  }
}

function MapWatcher({ onUpdate }: { onUpdate: () => void }) {
  useMapEvents({ moveend: onUpdate, zoomend: onUpdate });
  useEffect(() => { const id = setInterval(onUpdate, 10_000); return () => clearInterval(id); }, [onUpdate]);
  return null;
}

function DayNightBoundary({ coords, nightSide }: { coords: Array<{ lat: number; lon: number }>; nightSide: string }) {
  if (coords.length === 0) return null;

  // Render the boundary once per visible world copy so it repeats when the
  // map wraps horizontally (Leaflet only draws vector layers on the base copy).
  return (
    <>
      {WORLD_COPIES.map(offset => {
        const positions: [number, number][] = coords.map(c => [c.lat, c.lon + offset]);
        const edgeLat = nightSide === 'north' ? 90 : -90;
        const firstLon = positions[0][1];
        const lastLon = positions[positions.length - 1][1];
        const nightPolygon: [number, number][] = [
          ...positions,
          [edgeLat, lastLon],
          [edgeLat, firstLon]
        ];
        return (
          <Fragment key={offset}>
            <Polygon positions={nightPolygon} pathOptions={{ fillColor: '#1a2332', fillOpacity: 0.45, weight: 0, opacity: 0 }} />
            <Polygon positions={positions} pathOptions={{ weight: 4, color: '#fbbf24', opacity: 0.95, fillOpacity: 0, dashArray: '10, 6' }} />
          </Fragment>
        );
      })}
    </>
  );
}

function SatelliteMarker({ sat, zoom }: { sat: Satellite; zoom: number }) {
  const color = getCategoryColor(sat.category);
  // Draw a copy in each world rendition so data repeats on horizontal wrap.
  return (
    <>
      {WORLD_COPIES.map(offset => (
        <CircleMarker key={offset} center={[sat.lat, sat.lon + offset]} radius={4} pathOptions={{ fillColor: color, fillOpacity: 0.85, color: '#000', weight: 1.5 }}>
          <Tooltip direction="top" offset={[0, -10]} opacity={0.98}>
            <div style={{ fontFamily: 'system-ui', fontSize: '12px', background: '#ffffff', color: '#1a1a1a', padding: '8px', border: '1px solid #e5e7eb' }}>
              <div style={{ fontWeight: 'bold', fontSize: '14px', marginBottom: '6px', color }}>{sat.name}</div>
              <div><strong>Category:</strong> {getCategoryLabel(sat.category)}</div>
              <div><strong>Altitude:</strong> {sat.altitude.toFixed(0)} km</div>
            </div>
          </Tooltip>
        </CircleMarker>
      ))}
    </>
  );
}

function MetricsPanel({ metrics }: { metrics: SatelliteMetrics | null }) {
  if (!metrics) return null;
  return (
    <div style={{ position: 'absolute', top: '10px', right: '10px', backgroundColor: 'rgba(255,255,255,0.95)', color: '#1a1a1a', padding: '16px', borderRadius: '8px', border: '1px solid rgba(0,0,0,0.1)', boxShadow: '0 2px 8px rgba(0,0,0,0.1)', zIndex: 1000, minWidth: '280px' }}>
      <h3 style={{ marginBottom: '12px', fontSize: '16px', color: '#111', fontWeight: 600 }}>Satellite Statistics</h3>
      <div style={{ marginBottom: '12px', fontSize: '12px', color: '#666' }}>Last Update: {new Date(metrics.lastUpdate).toLocaleString()}</div>
      <strong>Total: {metrics.totalCount} satellites</strong>
      <div style={{ fontSize: '12px', marginTop: '12px' }}>
        {(metrics.categories ?? []).map(c => (
          <div key={c.category} style={{ marginBottom: '4px' }}>
            <span style={{ color: getCategoryColor(c.category) }}>●</span> {getCategoryLabel(c.category)}: <strong>{c.count}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function AppSatellite() {
  const [satellites, setSatellites] = useState<Satellite[]>([]);
  const [metrics, setMetrics] = useState<SatelliteMetrics | null>(null);
  const [terminatorCoords, setTerminatorCoords] = useState<Array<{ lat: number; lon: number }>>([]);
  const [nightSide, setNightSide] = useState<string>('south');
  const [status, setStatus] = useState('Loading...');
  const [zoom, setZoom] = useState(3);
  const mapRef = useRef<LeafletMap | null>(null);

  const loadData = useCallback(async () => {
    try {
      const data = await fetchAllSatellites();
      setSatellites(data);
      setStatus(`${data.length} satellites tracked`);
      if (mapRef.current) setZoom(mapRef.current.getZoom());
    } catch { setStatus('Failed to load'); }
  }, []);

  const loadMetrics = useCallback(async () => {
    try { setMetrics(await fetchMetrics()); } catch {}
  }, []);

  const loadTerminator = useCallback(async () => {
    try {
      const data = await fetchTerminator();
      setTerminatorCoords(data.coordinates);
      setNightSide(data.nightSide || 'south');
    } catch {}
  }, []);

  useEffect(() => {
    loadData();
    loadMetrics();
    loadTerminator();
    const d = setInterval(loadData, 10_000);
    const m = setInterval(loadMetrics, 30_000);
    const t = setInterval(loadTerminator, 60_000);
    return () => { clearInterval(d); clearInterval(m); clearInterval(t); };
  }, []);

  return (
    <div style={{ width: '100vw', height: '100vh' }}>
      <MapContainer center={[20, 0]} zoom={3} style={{ width: '100%', height: '100%' }} ref={mapRef} zoomControl={false}>
        <ZoomControl position="bottomright" />
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution='&copy; OpenStreetMap' />
        <MapWatcher onUpdate={loadData} />
        <DayNightBoundary coords={terminatorCoords} nightSide={nightSide} />
        {satellites.map(sat => <SatelliteMarker key={sat.id} sat={sat} zoom={zoom} />)}
      </MapContainer>
      <MetricsPanel metrics={metrics} />
      <div style={{ position: 'absolute', bottom: '10px', left: '10px', backgroundColor: 'rgba(255,255,255,0.95)', color: '#1a1a1a', padding: '8px 12px', borderRadius: '4px', border: '1px solid rgba(0,0,0,0.1)', boxShadow: '0 2px 8px rgba(0,0,0,0.1)', zIndex: 1000, fontSize: '12px' }}>{status}</div>
      <div style={{ position: 'absolute', top: '10px', left: '10px', backgroundColor: 'rgba(255,255,255,0.95)', color: '#1a1a1a', padding: '12px', borderRadius: '8px', border: '1px solid rgba(0,0,0,0.1)', boxShadow: '0 2px 8px rgba(0,0,0,0.1)', zIndex: 1000 }}>
        <h1 style={{ margin: 0, fontSize: '20px', color: '#111', fontWeight: 600 }}>proxima — Satellite Tracker</h1>
        <div style={{ fontSize: '13px', color: '#666', marginTop: '4px' }}>ISS + 200 satellites via gRPC</div>
      </div>
    </div>
  );
}
