import { useState, useEffect, useCallback, useRef } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, useMapEvents } from 'react-leaflet';
import type { Map as LeafletMap } from 'leaflet';
import 'leaflet/dist/leaflet.css';

// ── Types ──────────────────────────────────────────────────────────────────

interface Earthquake {
  id: string;
  lat: number;
  lon: number;
  depth: number;
  magnitude: number;
  place: string;
  time: string;
  alert: string | null;
  tsunami: number;
  url: string | null;
}

interface EarthquakeMetrics {
  totalCount: number;
  lastUpdate: string;
  magnitudeRanges: {
    minor: number;    // 2.5-3.9
    light: number;    // 4.0-4.9
    moderate: number; // 5.0-5.9
    strong: number;   // 6.0-6.9
    major: number;    // 7.0-7.9
    great: number;    // 8.0+
  };
  recentLarge: Array<{
    id: string;
    magnitude: number;
    place: string;
    time: string;
    alert: string | null;
    tsunami: number;
  }>;
}

// ── API Client ─────────────────────────────────────────────────────────────

async function fetchAllEarthquakes(): Promise<Earthquake[]> {
  const res = await fetch('/api/earthquakes');
  const data = await res.json();
  return data.earthquakes || [];
}

async function fetchMetrics(): Promise<EarthquakeMetrics> {
  const res = await fetch('/api/metrics');
  return await res.json();
}

// ── Magnitude Styling ──────────────────────────────────────────────────────

function getMagnitudeColor(mag: number): string {
  if (mag >= 8.0) return '#8B0000'; // Great — dark red
  if (mag >= 7.0) return '#DC143C'; // Major — crimson
  if (mag >= 6.0) return '#FF4500'; // Strong — orange red
  if (mag >= 5.0) return '#FF8C00'; // Moderate — dark orange
  if (mag >= 4.0) return '#FFA500'; // Light — orange
  if (mag >= 3.0) return '#FFD700'; // Minor — gold
  return '#F0E68C';                  // Micro — khaki
}

function getMagnitudeRadius(mag: number, zoom: number): number {
  // Scale radius by magnitude and zoom level
  const base = Math.max(3, mag * 2);
  const zoomFactor = Math.max(0.5, zoom / 8);
  return base * zoomFactor;
}

function getMagnitudeLabel(mag: number): string {
  if (mag >= 8.0) return 'Great';
  if (mag >= 7.0) return 'Major';
  if (mag >= 6.0) return 'Strong';
  if (mag >= 5.0) return 'Moderate';
  if (mag >= 4.0) return 'Light';
  if (mag >= 3.0) return 'Minor';
  return 'Micro';
}

function getAlertColor(alert: string | null): string {
  if (!alert) return 'transparent';
  switch (alert.toLowerCase()) {
    case 'green': return '#90EE90';
    case 'yellow': return '#FFD700';
    case 'orange': return '#FF8C00';
    case 'red': return '#DC143C';
    default: return 'transparent';
  }
}

// ── Map Bounds Watcher ─────────────────────────────────────────────────────

function MapWatcher({ onUpdate }: { onUpdate: () => void }) {
  const map = useMapEvents({
    moveend: onUpdate,
    zoomend: onUpdate,
  });
  
  useEffect(() => {
    onUpdate();
    const id = setInterval(onUpdate, 300_000); // Refresh every 5 minutes
    return () => clearInterval(id);
  }, [onUpdate]);
  
  return null;
}

// ── Earthquake Marker ──────────────────────────────────────────────────────

function EarthquakeMarker({ eq, zoom }: { eq: Earthquake; zoom: number }) {
  const color = getMagnitudeColor(eq.magnitude);
  const radius = getMagnitudeRadius(eq.magnitude, zoom);
  const alertColor = getAlertColor(eq.alert);
  
  return (
    <CircleMarker
      center={[eq.lat, eq.lon]}
      radius={radius}
      pathOptions={{
        fillColor: color,
        fillOpacity: 0.6,
        color: alertColor,
        weight: alertColor !== 'transparent' ? 3 : 1,
        opacity: 0.9,
      }}
    >
      <Popup maxWidth={300}>
        <div style={{ fontFamily: 'system-ui, sans-serif', fontSize: '13px' }}>
          <div style={{
            fontWeight: 'bold',
            fontSize: '16px',
            marginBottom: '8px',
            color: color
          }}>
            M{eq.magnitude.toFixed(1)} — {getMagnitudeLabel(eq.magnitude)}
          </div>
          <div style={{ marginBottom: '6px' }}>
            <strong>Location:</strong> {eq.place}
          </div>
          <div style={{ marginBottom: '6px' }}>
            <strong>Time:</strong> {new Date(eq.time).toLocaleString()}
          </div>
          <div style={{ marginBottom: '6px' }}>
            <strong>Depth:</strong> {eq.depth.toFixed(1)} km
          </div>
          <div style={{ marginBottom: '6px' }}>
            <strong>Coordinates:</strong> {eq.lat.toFixed(4)}°, {eq.lon.toFixed(4)}°
          </div>
          {eq.alert && (
            <div style={{ marginBottom: '6px' }}>
              <strong>Alert:</strong>{' '}
              <span style={{
                padding: '2px 6px',
                borderRadius: '3px',
                backgroundColor: alertColor,
                color: '#000',
                fontWeight: 'bold'
              }}>
                {eq.alert.toUpperCase()}
              </span>
            </div>
          )}
          {eq.tsunami > 0 && (
            <div style={{
              marginBottom: '6px',
              padding: '4px 8px',
              backgroundColor: '#FFD700',
              borderRadius: '3px',
              fontWeight: 'bold',
              color: '#000'
            }}>
              🌊 TSUNAMI WARNING
            </div>
          )}
          {eq.url && (
            <div style={{ marginTop: '8px' }}>
              <a
                href={eq.url}
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: '#007bff', textDecoration: 'none' }}
              >
                View Details on USGS →
              </a>
            </div>
          )}
        </div>
      </Popup>
    </CircleMarker>
  );
}

// ── Metrics Panel ──────────────────────────────────────────────────────────

function MetricsPanel({ metrics }: { metrics: EarthquakeMetrics | null }) {
  if (!metrics) return null;

  const ranges = metrics.magnitudeRanges;
  const total = ranges.minor + ranges.light + ranges.moderate + ranges.strong + ranges.major + ranges.great;

  return (
    <div style={{
      position: 'absolute',
      top: '10px',
      right: '10px',
      backgroundColor: 'rgba(255, 255, 255, 0.95)',
      padding: '16px',
      borderRadius: '8px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
      zIndex: 1000,
      fontFamily: 'system-ui, sans-serif',
      fontSize: '13px',
      minWidth: '280px',
      maxHeight: '90vh',
      overflowY: 'auto'
    }}>
      <h3 style={{ marginBottom: '12px', fontSize: '16px', fontWeight: 'bold' }}>
        Earthquake Statistics
      </h3>
      
      <div style={{ marginBottom: '12px', fontSize: '12px', color: '#666' }}>
        Last Update: {new Date(metrics.lastUpdate).toLocaleString()}
      </div>

      <div style={{ marginBottom: '16px' }}>
        <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>
          Total: {total} earthquakes (past 24h)
        </div>
        
        <div style={{ fontSize: '12px' }}>
          <div style={{ marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: getMagnitudeColor(3.0) }}>● Minor (2.5-3.9):</span>
            <strong>{ranges.minor}</strong>
          </div>
          <div style={{ marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: getMagnitudeColor(4.0) }}>● Light (4.0-4.9):</span>
            <strong>{ranges.light}</strong>
          </div>
          <div style={{ marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: getMagnitudeColor(5.0) }}>● Moderate (5.0-5.9):</span>
            <strong>{ranges.moderate}</strong>
          </div>
          <div style={{ marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: getMagnitudeColor(6.0) }}>● Strong (6.0-6.9):</span>
            <strong>{ranges.strong}</strong>
          </div>
          <div style={{ marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: getMagnitudeColor(7.0) }}>● Major (7.0-7.9):</span>
            <strong>{ranges.major}</strong>
          </div>
          <div style={{ marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: getMagnitudeColor(8.0) }}>● Great (8.0+):</span>
            <strong>{ranges.great}</strong>
          </div>
        </div>
      </div>

      {metrics.recentLarge.length > 0 && (
        <div>
          <div style={{ fontWeight: 'bold', marginBottom: '8px', borderTop: '1px solid #ddd', paddingTop: '12px' }}>
            Recent Large Quakes (M ≥ 5.0)
          </div>
          <div style={{ fontSize: '11px', maxHeight: '300px', overflowY: 'auto' }}>
            {metrics.recentLarge.map((eq) => (
              <div key={eq.id} style={{
                marginBottom: '8px',
                padding: '6px',
                backgroundColor: '#f8f9fa',
                borderRadius: '4px',
                borderLeft: `3px solid ${getMagnitudeColor(eq.magnitude)}`
              }}>
                <div style={{ fontWeight: 'bold', marginBottom: '2px' }}>
                  M{eq.magnitude.toFixed(1)} — {eq.place}
                </div>
                <div style={{ color: '#666' }}>
                  {new Date(eq.time).toLocaleString()}
                </div>
                {eq.alert && (
                  <div style={{ marginTop: '2px' }}>
                    <span style={{
                      padding: '1px 4px',
                      borderRadius: '2px',
                      backgroundColor: getAlertColor(eq.alert),
                      color: '#000',
                      fontSize: '10px',
                      fontWeight: 'bold'
                    }}>
                      {eq.alert.toUpperCase()}
                    </span>
                  </div>
                )}
                {eq.tsunami > 0 && (
                  <div style={{ marginTop: '2px', fontSize: '10px', color: '#ff6b00' }}>
                    🌊 Tsunami
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      <div style={{
        marginTop: '12px',
        paddingTop: '12px',
        borderTop: '1px solid #ddd',
        fontSize: '11px',
        color: '#666',
        textAlign: 'center'
      }}>
        Data from USGS Earthquake Hazards Program
        <br />
        Updates every 5 minutes
      </div>
    </div>
  );
}

// ── Main App ───────────────────────────────────────────────────────────────

export default function AppEarthquake() {
  const [earthquakes, setEarthquakes] = useState<Earthquake[]>([]);
  const [metrics, setMetrics] = useState<EarthquakeMetrics | null>(null);
  const [status, setStatus] = useState('Loading earthquake data...');
  const [zoom, setZoom] = useState(3);
  const mapRef = useRef<LeafletMap | null>(null);

  const loadData = useCallback(async () => {
    try {
      const data = await fetchAllEarthquakes();
      setEarthquakes(data);
      setStatus(`${data.length} earthquakes (past 24h, M ≥ 2.5) · ${new Date().toLocaleTimeString()}`);
      
      // Update zoom from map if available
      if (mapRef.current) {
        setZoom(mapRef.current.getZoom());
      }
    } catch (err) {
      console.error('Failed to fetch earthquakes:', err);
      setStatus('Failed to load earthquake data');
    }
  }, []);

  const loadMetrics = useCallback(async () => {
    try {
      const data = await fetchMetrics();
      setMetrics(data);
    } catch (err) {
      console.error('Failed to fetch metrics:', err);
    }
  }, []);

  useEffect(() => {
    loadData();
    loadMetrics();
    
    // Poll every 5 minutes (matches USGS update frequency)
    const dataInterval = setInterval(loadData, 300_000);
    const metricsInterval = setInterval(loadMetrics, 60_000);
    
    return () => {
      clearInterval(dataInterval);
      clearInterval(metricsInterval);
    };
  }, [loadData, loadMetrics]);

  return (
    <div style={{ position: 'relative', width: '100vw', height: '100vh' }}>
      <MapContainer
        center={[20, 0]}
        zoom={3}
        style={{ width: '100%', height: '100%' }}
        ref={mapRef}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <MapWatcher onUpdate={loadData} />
        
        {earthquakes.map((eq) => (
          <EarthquakeMarker key={eq.id} eq={eq} zoom={zoom} />
        ))}
      </MapContainer>

      <MetricsPanel metrics={metrics} />

      <div style={{
        position: 'absolute',
        bottom: '10px',
        left: '10px',
        backgroundColor: 'rgba(255, 255, 255, 0.95)',
        padding: '8px 12px',
        borderRadius: '4px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
        zIndex: 1000,
        fontFamily: 'system-ui, sans-serif',
        fontSize: '12px'
      }}>
        {status}
      </div>

      <div style={{
        position: 'absolute',
        top: '10px',
        left: '10px',
        backgroundColor: 'rgba(255, 255, 255, 0.95)',
        padding: '12px',
        borderRadius: '8px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
        zIndex: 1000,
        fontFamily: 'system-ui, sans-serif'
      }}>
        <h1 style={{ margin: 0, fontSize: '20px', fontWeight: 'bold' }}>
          geo-redis — Earthquake Tracker
        </h1>
        <div style={{ fontSize: '13px', color: '#666', marginTop: '4px' }}>
          Real-time seismic data via gRPC
        </div>
      </div>
    </div>
  );
}
