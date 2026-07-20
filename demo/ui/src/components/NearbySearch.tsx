import { useState, useRef } from 'react';
import type { Map as LeafletMap } from 'leaflet';
import { geocodePlace, fetchNearby } from '../api/client';
import type { NearbyResult } from '../types';

interface Props {
  mapRef: React.RefObject<LeafletMap | null>;
  /** Called when the user clicks a result row so the parent can highlight it. */
  onSelect?: (id: string, lat: number, lon: number) => void;
}

function fmtDist(m: number): string {
  return m >= 1000 ? `${(m / 1000).toFixed(0)} km` : `${m.toFixed(0)} m`;
}

export default function NearbySearch({ mapRef, onSelect }: Props) {
  const [query,   setQuery]   = useState('');
  const [results, setResults] = useState<NearbyResult[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    const q = query.trim();
    if (!q) return;

    setLoading(true);
    setError(null);
    setResults(null);

    const coords = await geocodePlace(q);
    if (!coords) {
      setError(`Location "${q}" not found`);
      setLoading(false);
      return;
    }

    const [lat, lon] = coords;

    // Pan the map to the geocoded location
    mapRef.current?.flyTo([lat, lon], 7, { duration: 1.2 });

    try {
      const res = await fetchNearby(lat, lon, 500_000, 20);
      setResults(res.results);
      if (res.results.length === 0) setError('No aircraft within 500 km');
    } catch {
      setError('Query failed — is the server running?');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={styles.container}>
      <form onSubmit={handleSearch} style={styles.form}>
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Search location… e.g. Tokyo"
          style={styles.input}
          disabled={loading}
        />
        <button type="submit" style={styles.btn} disabled={loading || !query.trim()}>
          {loading ? '…' : '🔍'}
        </button>
      </form>

      {error && <div style={styles.error}>{error}</div>}

      {results && results.length > 0 && (
        <div style={styles.list}>
          <div style={styles.listHeader}>
            {results.length} nearest aircraft
          </div>
          {results.map(r => {
            const callsign = r.entry.payload?.callsign ?? r.entry.id;
            const alt      = r.entry.payload?.altitude;
            return (
              <div
                key={r.entry.id}
                style={styles.row}
                onClick={() => {
                  mapRef.current?.flyTo([r.entry.lat, r.entry.lon], 10, { duration: 0.8 });
                  onSelect?.(r.entry.id, r.entry.lat, r.entry.lon);
                }}
              >
                <span style={styles.callsign}>{callsign}</span>
                <span style={styles.meta}>
                  {alt != null ? `${Math.round(alt).toLocaleString()} m · ` : ''}
                  {fmtDist(r.distance_m)}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    position:        'absolute',
    top:             10,
    left:            60,
    zIndex:          1000,
    width:           280,
    fontFamily:      'system-ui, sans-serif',
    fontSize:        13,
  },
  form: {
    display:         'flex',
    gap:             4,
  },
  input: {
    flex:            1,
    padding:         '6px 10px',
    borderRadius:    6,
    border:          '1px solid #555',
    background:      'rgba(24,24,32,0.92)',
    color:           '#e2e8f0',
    outline:         'none',
    fontSize:        13,
  },
  btn: {
    padding:         '6px 10px',
    borderRadius:    6,
    border:          'none',
    background:      '#3b82f6',
    color:           '#fff',
    cursor:          'pointer',
    fontSize:        14,
  },
  error: {
    marginTop:       6,
    padding:         '5px 10px',
    borderRadius:    6,
    background:      'rgba(239,68,68,0.85)',
    color:           '#fff',
  },
  list: {
    marginTop:       6,
    background:      'rgba(15,15,25,0.93)',
    borderRadius:    8,
    border:          '1px solid #333',
    maxHeight:       320,
    overflowY:       'auto',
  },
  listHeader: {
    padding:         '6px 12px',
    color:           '#94a3b8',
    borderBottom:    '1px solid #2d2d3a',
    fontWeight:      600,
    fontSize:        11,
    textTransform:   'uppercase',
    letterSpacing:   '0.05em',
  },
  row: {
    display:         'flex',
    justifyContent:  'space-between',
    alignItems:      'center',
    padding:         '7px 12px',
    cursor:          'pointer',
    borderBottom:    '1px solid #1e1e2a',
    transition:      'background 0.12s',
  },
  callsign: {
    color:           '#e2e8f0',
    fontWeight:      500,
  },
  meta: {
    color:           '#64748b',
    fontSize:        11,
  },
};
