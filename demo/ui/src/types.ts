export interface AircraftPayload {
  callsign?:       string | null;
  altitude?:       number | null;
  velocity?:       number | null;
  heading?:        number | null;
  on_ground?:      boolean | null;
  origin_country?: string | null;
  history?:        [number, number][];
  // ── Weather-station extras (set by weather-server, absent for aircraft) ──
  /** True when this entry comes from the weather server */
  __is_weather?:    boolean;
  /** Raw temperature °C */
  temp_c?:          number | null;
  /** Apparent / feels-like temperature °C */
  feels_like_c?:    number | null;
  /** Relative humidity % */
  humidity_pct?:    number | null;
  /** Wind gust speed knots */
  gust_kt?:         number | null;
  /** Cloud cover % */
  cloud_pct?:       number | null;
  /** Surface pressure hPa */
  pressure_hpa?:    number | null;
  /** Precipitation mm/h */
  precip?:          number | null;
  /** Number of raw METAR stations aggregated into this cluster node */
  count?:           number | null;
  /** WMO weather interpretation code (0=clear … 99=thunderstorm+hail) */
  wmo_code?:        number | null;
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
