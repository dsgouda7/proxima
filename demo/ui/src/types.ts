export interface AircraftPayload {
  callsign?:       string | null;
  altitude?:       number | null;
  velocity?:       number | null;
  heading?:        number | null;
  on_ground?:      boolean | null;
  origin_country?: string | null;
  /**
   * Last 1–3 positions [lat, lon], oldest first.
   * Populated ONLY after a detail fetch from SQLite (zoom ≥ 9, < 5 aircraft).
   * Absent in normal map markers.
   */
  history?:        [number, number][];
}

export interface Aircraft {
  id:      string;
  lat:     number;
  lon:     number;
  payload: AircraftPayload;
}

export type AircraftType = 'widebody' | 'narrowbody' | 'turboprop' | 'helicopter' | 'small' | 'ground';

export interface AircraftResponse { count: number; aircraft: Aircraft[]; }

export interface MetricsSnapshot {
  write_count:  number;
  write_avg_us: number;
  write_max_us: number;
  read_count:   number;
  read_avg_us:  number;
  read_max_us:  number;
}

export interface MetricsResponse {
  metrics:    MetricsSnapshot;
  trie_size:  number;
  last_sync?: number;
}
