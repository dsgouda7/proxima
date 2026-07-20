export type { Aircraft, AircraftResponse, MetricsSnapshot, MetricsResponse, TrieSnapshot, NearbyResponse, NearbyResult } from '../types';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${path}`);
  return res.json() as Promise<T>;
}

export const fetchAllAircraft = (zoom?: number) =>
  get<import('../types').AircraftResponse>(
    zoom != null ? `/api/aircraft?zoom=${zoom}` : '/api/aircraft'
  );

export const fetchMetrics = () =>
  get<import('../types').MetricsResponse>('/api/metrics');

export const fetchTrieSnapshot = () =>
  get<import('../types').TrieSnapshot>('/api/trie');

export function fetchRegion(s: number, w: number, n: number, e: number, zoom?: number) {
  const base = `/api/region?s=${s}&w=${w}&n=${n}&e=${e}`;
  return get<import('../types').AircraftResponse>(zoom != null ? `${base}&zoom=${zoom}` : base);
}

/** Geocode a place name using the Nominatim OpenStreetMap API.
 *  Returns [lat, lon] or null if no result found. */
export async function geocodePlace(query: string): Promise<[number, number] | null> {
  const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`;
  try {
    const res = await fetch(url, { headers: { 'Accept-Language': 'en' } });
    if (!res.ok) return null;
    const data = await res.json() as { lat: string; lon: string }[];
    if (!data.length) return null;
    return [parseFloat(data[0].lat), parseFloat(data[0].lon)];
  } catch {
    return null;
  }
}

/** Find the nearest aircraft (or other entities) within `radius_m` metres of `(lat, lon)`. */
export function fetchNearby(lat: number, lon: number, radius_m = 500_000, limit = 20) {
  return get<import('../types').NearbyResponse>(
    `/api/nearby?lat=${lat}&lon=${lon}&radius_m=${radius_m}&limit=${limit}`
  );
}

export interface AircraftDetail {
  id:             string;
  callsign?:      string | null;
  origin_country: string;
  altitude?:      number | null;
  velocity?:      number | null;
  heading?:       number | null;
  on_ground:      boolean;
  /** Last ≤3 positions [lat, lon], oldest first. */
  history:        [number, number][];
}

/** Fetches full metadata + position history from SQLite (server-side).
 *  Only call this when < 5 aircraft are in view and zoom >= 9. */
export async function fetchAircraftDetail(id: string): Promise<AircraftDetail | null> {
  try {
    const res = await fetch(`/api/aircraft/${encodeURIComponent(id)}`);
    if (!res.ok) return null;
    const data = await res.json() as AircraftDetail & { error?: string };
    if (data.error) return null;
    return data;
  } catch {
    return null;
  }
}
