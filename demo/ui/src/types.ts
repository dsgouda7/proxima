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

  // ── Radio-station extras ──────────────────────────────────────────────────
  /** True when this entry comes from the radio server */
  __is_radio?:      boolean;
  /** Dominant genre tags from the top station (comma-separated) */
  top_tags?:        string | null;
  /** ISO 3166-1 alpha-2 country code of the top station */
  top_cc?:          string | null;
  /** For leaf-level cell markers: every station in this S2 cell */
  stations?:        RadioStationInfo[] | null;
}

/** Compact station record embedded in a leaf-level radio cluster. */
export interface RadioStationInfo {
  uuid:        string;
  name:        string;
  stream_url:  string;
  tags:        string;
  country:     string;
  countrycode: string;
  codec:       string;
  bitrate:     number;
  votes:       number;
  favicon:     string;
}

export interface Aircraft {
  id:      string;
  lat:     number;
  lon:     number;
  payload: AircraftPayload;
}

export type AircraftType = 'widebody' | 'narrowbody' | 'turboprop' | 'helicopter' | 'small' | 'ground';

/** One occupied leaf in the in-memory S2 trie — an entity plus its cell token. */
export interface TrieNode {
  id:    string;
  token: string;
}

/** Flat snapshot of every entry currently held in the S2 trie. */
export interface TrieSnapshot {
  s2_level: number;
  count:    number;
  nodes:    TrieNode[];
}

export interface AircraftResponse { count: number; aircraft: Aircraft[]; }

/** One result from the /api/nearby endpoint. */
export interface NearbyResult {
  distance_m: number;
  entry: Aircraft;
}

/** Response shape from GET /api/nearby */
export interface NearbyResponse {
  count:     number;
  query_lat: number;
  query_lon: number;
  radius_m:  number;
  results:   NearbyResult[];
}

export interface MetricsSnapshot {
  write_count:  number;
  write_avg_us: number;
  write_max_us: number;
  read_count:   number;
  read_avg_us:  number;
  read_max_us:  number;
  /** Present when query_nearby has been called at least once. */
  nearby_count?:   number;
  nearby_avg_us?:  number;
  nearby_max_us?:  number;
}

export interface MetricsResponse {
  metrics:    MetricsSnapshot;
  trie_size:  number;
  last_sync?: number;
}
