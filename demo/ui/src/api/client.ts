export type { Aircraft, AircraftResponse, MetricsSnapshot, MetricsResponse } from '../types';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${path}`);
  return res.json() as Promise<T>;
}

export const fetchAllAircraft = () =>
  get<import('../types').AircraftResponse>('/api/aircraft');

export const fetchMetrics = () =>
  get<import('../types').MetricsResponse>('/api/metrics');

export function fetchRegion(s: number, w: number, n: number, e: number) {
  return get<import('../types').AircraftResponse>(`/api/region?s=${s}&w=${w}&n=${n}&e=${e}`);
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
