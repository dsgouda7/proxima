import { useState, useEffect, useCallback, useRef } from 'react';
import { MapContainer, TileLayer, useMapEvents } from 'react-leaflet';
import type { Map as LeafletMap } from 'leaflet';
import { fetchAllAircraft, fetchRegion, fetchMetrics } from './api/client';
import { Aircraft, MetricsResponse, RadioStationInfo } from './types';
import RadioMarker from './components/RadioMarker';
import RadioFlyout from './components/RadioFlyout';
import MetricsPanel from './components/MetricsPanel';
import 'leaflet/dist/leaflet.css';

const REGION_ZOOM = 4;   // switch to viewport-filtered query above this zoom
const LEAF_ZOOM   = 8;   // server includes station lists in payload at this zoom

function clampBounds(s: number, w: number, n: number, e: number) {
  if (e - w >= 358) return { s: -85, w: -180, n: 85, e: 180 };
  return { s, w: Math.max(-180, Math.min(180, w)), n, e: Math.max(-180, Math.min(180, e)) };
}

function MapWatcher({ onBounds }: { onBounds: (s:number,w:number,n:number,e:number,z:number)=>void }) {
  const map = useMapEvents({
    moveend: () => fire(map, onBounds),
    zoomend: () => fire(map, onBounds),
  });
  useEffect(() => {
    fire(map, onBounds);
    const id = setInterval(() => fire(map, onBounds), 120_000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return null;
}

function fire(map: LeafletMap, cb: (s:number,w:number,n:number,e:number,z:number)=>void) {
  const b = map.getBounds();
  cb(b.getSouth(), b.getWest(), b.getNorth(), b.getEast(), map.getZoom());
}

export default function AppRadio() {
  const [clusters,       setClusters]       = useState<Aircraft[]>([]);
  const [metrics,        setMetrics]        = useState<MetricsResponse | null>(null);
  const [status,         setStatus]         = useState('Loading radio stations…');
  const [zoom,           setZoom]           = useState(3);
  const [selected,       setSelected]       = useState<Aircraft | null>(null);
  const [playing,        setPlaying]        = useState<string | null>(null);
  const [playingStation, setPlayingStation] = useState<RadioStationInfo | null>(null);
  const [playError,      setPlayError]      = useState<string | null>(null);
  const mapRef   = useRef<LeafletMap | null>(null);
  const audioRef = useRef<HTMLAudioElement | null>(null);

  const isLeaf = zoom >= LEAF_ZOOM;

  const handleBounds = useCallback(async (
    s: number, w: number, n: number, e: number, z: number
  ) => {
    setZoom(z);
    // Close flyout when zooming out past leaf level.
    if (z < LEAF_ZOOM) setSelected(null);

    try {
      const { s: cs, w: cw, n: cn, e: ce } = clampBounds(s, w, n, e);
      const t0 = performance.now();
      const res = z >= REGION_ZOOM
        ? await fetchRegion(cs, cw, cn, ce, z)
        : await fetchAllAircraft(z);
      const queryMs = Math.round(performance.now() - t0);

      setClusters(res.aircraft);
      setStatus(
        z >= LEAF_ZOOM
          ? `${res.count.toLocaleString()} station groups · ${queryMs}ms · click to play`
          : `${res.count.toLocaleString()} clusters · ${queryMs}ms spatial query · zoom in to browse`
      );
    } catch {
      setStatus('API unreachable');
    }
  }, []);

  const handleMarkerClick = useCallback((a: Aircraft) => {
    setSelected(a);
    mapRef.current?.flyTo([a.lat, a.lon], Math.max(mapRef.current.getZoom(), LEAF_ZOOM), {
      animate: true, duration: 0.7,
    });
  }, []);

  const handlePlay = useCallback((station: RadioStationInfo) => {
    if (audioRef.current) {
      audioRef.current.pause();
      audioRef.current.src = '';
      audioRef.current = null;
    }
    setPlayError(null);
    if (playing === station.uuid) {
      setPlaying(null);
      setPlayingStation(null);
      return;
    }
    const audio = new Audio(station.stream_url);
    audio.onerror = () => {
      setPlayError(`Can't connect to “${station.name}”`);
      setPlaying(null);
      setPlayingStation(null);
      audioRef.current = null;
    };
    audio.play().catch(() => {});
    audioRef.current = audio;
    setPlaying(station.uuid);
    setPlayingStation(station);
  }, [playing]);

  const handleStop = useCallback(() => {
    if (audioRef.current) {
      audioRef.current.pause();
      audioRef.current.src = '';
      audioRef.current = null;
    }
    setPlaying(null);
    setPlayingStation(null);
    setPlayError(null);
  }, []);

  // Set tab title once on mount.
  useEffect(() => { document.title = 'geo-redis — Radio Explorer'; }, []);

  // Metrics poller.
  useEffect(() => {
    const poll = async () => { try { setMetrics(await fetchMetrics()); } catch { /**/ } };
    void poll();
    const id = setInterval(poll, 30_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', background: '#020617' }}>
      <header style={{
        padding: '7px 16px',
        background: 'linear-gradient(90deg,#1e1b4b,#0f172a)',
        borderBottom: '1px solid rgba(129,140,248,0.2)',
        display: 'flex', alignItems: 'center', gap: 12, flexShrink: 0,
        boxShadow: '0 2px 12px rgba(0,0,0,0.5)',
      }}>
        <span style={{ fontSize: 20 }}>📻</span>
        <strong style={{ fontSize: '1rem', color: '#818cf8', letterSpacing: 0.5 }}>geo-redis</strong>
        <span style={{ color: '#475569', fontSize: 12 }}>·</span>
        <span style={{ color: '#94a3b8', fontSize: 12 }}>Live Radio Explorer</span>
        <span style={{
          marginLeft: 'auto', fontSize: '0.72rem', color: '#64748b',
          maxWidth: 380, textAlign: 'right', whiteSpace: 'nowrap',
          overflow: 'hidden', textOverflow: 'ellipsis',
        }}>{status}</span>
        {playingStation && (
          <div style={{
            display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0,
            background: 'rgba(99,102,241,0.15)',
            border: '1px solid rgba(99,102,241,0.35)',
            borderRadius: 20, padding: '3px 10px 3px 8px',
            fontSize: 11, color: '#a5b4fc',
          }}>
            <span style={{ fontSize: 8, color: '#6366f1', animation: 'pulse 1.5s ease-in-out infinite' }}>●</span>
            <span style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {playingStation.name}
            </span>
            <button
              onClick={handleStop}
              style={{ background: 'none', border: 'none', color: '#818cf8', cursor: 'pointer', fontSize: 13, padding: '0 2px', lineHeight: 1, flexShrink: 0 }}
              title="Stop"
            >⧥</button>
          </div>
        )}
        {playError && !playingStation && (
          <div style={{ fontSize: 10, color: '#f87171', flexShrink: 0, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            ⚠ {playError}
          </div>
        )}
      </header>

      <div style={{ flex: 1, position: 'relative' }}>
        <MapContainer
          center={[20, 0]}
          zoom={3}
          minZoom={-1}
          style={{ height: '100%', width: '100%', background: '#0c1a2e' }}
          ref={mapRef}
          worldCopyJump={true}
        >
          <TileLayer
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            attribution='&copy; OSM &copy; CARTO'
            minZoom={0} maxZoom={19}
          />
          <MapWatcher onBounds={handleBounds} />
          {clusters.map(a => (
            <RadioMarker
              key={a.id}
              cluster={a}
              isLeaf={isLeaf}
              onClick={isLeaf ? handleMarkerClick : undefined}
            />
          ))}
        </MapContainer>

        {/* Flyout panel — only rendered for a leaf-level selection */}
        {selected && isLeaf && (
          <RadioFlyout
            cluster={selected}
            onClose={() => setSelected(null)}
            playing={playing}
            onPlay={handlePlay}
            playError={playError}
          />
        )}

        {metrics && (
          <MetricsPanel
            metrics={metrics}
            entityLabel="Stations"
            title="S2 Index"
            hideLatency
          />
        )}

        {/* Legend */}
        <div style={{
          position: 'absolute', bottom: 24, left: 10, zIndex: 1000,
          background: 'rgba(15,23,42,0.9)', borderRadius: 8,
          padding: '8px 12px', fontSize: 10, color: '#94a3b8',
          border: '1px solid rgba(255,255,255,0.07)',
          backdropFilter: 'blur(4px)',
        }}>
          <div style={{ fontWeight: 700, marginBottom: 5, color: '#e2e8f0', fontSize: 11 }}>
            Radio Explorer
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: 'rgba(168,85,247,0.6)', border: '1.5px solid rgba(168,85,247,0.6)', flexShrink: 0 }} />
            <span>Cluster (zoom in)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: 'rgba(99,102,241,0.3)', border: '1.5px solid rgba(129,140,248,0.9)', flexShrink: 0 }} />
            <span>Leaf — click to browse &amp; play</span>
          </div>
          <div style={{ marginTop: 6, fontSize: 9, color: '#334155', borderTop: '1px solid rgba(255,255,255,0.05)', paddingTop: 5 }}>
            Zoom {LEAF_ZOOM}+ · individual station groups
          </div>
        </div>
      </div>
    </div>
  );
}
