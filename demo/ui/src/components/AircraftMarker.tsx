import { useMemo } from 'react';
import { Marker, Polyline, Tooltip } from 'react-leaflet';
import L from 'leaflet';
import { Aircraft } from '../types';
import {
  getAltitudeColor, getAircraftType, getTypeLabel, getTypeImage,
  fmtAlt, fmtSpeed, countryFlag, bearing,
} from '../utils/aircraft';

// ── Plane SVG path — top-down view, points NORTH (up). Rotated by heading. ──
const PLANE_PATH = 'M12,1 L9,7 L1,10 L1,12 L9,10 L10,20 L7,22 L7,23 L12,21 L17,23 L17,22 L14,20 L15,10 L23,12 L23,10 L15,7 Z';

function makePlaneIcon(heading: number | null | undefined, color: string, size = 26): L.DivIcon {
  const h = heading ?? 0;
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}"
      viewBox="0 0 24 24"
      style="transform:rotate(${h}deg);filter:drop-shadow(0 1px 3px rgba(0,0,0,0.7));display:block">
    <path d="${PLANE_PATH}" fill="${color}" stroke="rgba(255,255,255,0.35)" stroke-width="0.6"/>
  </svg>`;
  return L.divIcon({
    html: svg,
    className: '',
    iconSize:   [size, size],
    iconAnchor: [size / 2, size / 2],
    tooltipAnchor: [size / 2 + 4, -size / 2],
  });
}

function makeGroundIcon(size = 18): L.DivIcon {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 24 24">
    <circle cx="12" cy="12" r="9" fill="#475569" stroke="rgba(255,255,255,0.25)" stroke-width="1"/>
    <path d="${PLANE_PATH}" fill="#94a3b8" transform="scale(0.7) translate(5,5)"/>
  </svg>`;
  return L.divIcon({
    html: svg, className: '', iconSize: [size, size], iconAnchor: [size / 2, size / 2],
    tooltipAnchor: [size / 2 + 4, -size / 2],
  });
}

// ── Weather station icon ───────────────────────────────────────────────────

const WMO_EMOJI: Record<number, string> = {
  0:'☀️', 1:'🌤️', 2:'⛅', 3:'☁️',
  45:'🌫️', 48:'🌫️',
  51:'🌦️', 53:'🌦️', 55:'🌧️',
  61:'🌧️', 63:'🌧️', 65:'🌧️', 66:'🌨️', 67:'🌨️',
  71:'❄️', 73:'❄️', 75:'❄️', 77:'🌨️',
  80:'🌦️', 81:'🌦️', 82:'🌧️',
  85:'🌨️', 86:'🌨️',
  95:'⛈️', 96:'⛈️', 99:'⛈️',
};

function getTempColor(tempC: number): string {
  if (tempC >= 35) return '#ef4444';   // red    — very hot
  if (tempC >= 25) return '#f97316';   // orange — hot
  if (tempC >= 15) return '#eab308';   // yellow — warm
  if (tempC >=  5) return '#22c55e';   // green  — mild
  if (tempC >= -5) return '#06b6d4';   // cyan   — cool
  if (tempC >=-20) return '#3b82f6';   // blue   — cold
  return '#a855f7';                    // purple — very cold
}

function makeWeatherIcon(wmoCode: number, tempC: number, windDir: number | null | undefined, count = 1): L.DivIcon {
  const emoji  = WMO_EMOJI[wmoCode] ?? '🌡️';
  const bg     = getTempColor(tempC);
  const temp   = Math.round(tempC);
  const sign   = temp > 0 ? '+' : '';
  // Scale size slightly by station count (log scale so it doesn't get enormous)
  const size   = Math.min(52, 34 + Math.floor(Math.log2(count + 1) * 2));
  const wdArrow = windDir != null
    ? `<div style="position:absolute;top:1px;right:2px;font-size:9px;line-height:1;transform:rotate(${windDir}deg)">↑</div>`
    : '';
  const countBadge = count > 1
    ? `<div style="position:absolute;bottom:-6px;left:50%;transform:translateX(-50%);background:#0f172a;color:#94a3b8;font-size:7px;font-weight:700;padding:1px 4px;border-radius:6px;white-space:nowrap;border:1px solid rgba(255,255,255,0.15)">${count}</div>`
    : '';
  const html = `<div style="
      position:relative;
      width:${size}px;height:${size}px;
      background:${bg};
      border-radius:50%;
      display:flex;flex-direction:column;
      align-items:center;justify-content:center;
      border:1.5px solid rgba(255,255,255,0.5);
      box-shadow:0 2px 6px rgba(0,0,0,0.55);
      line-height:1;
    ">
    ${wdArrow}
    <span style="font-size:${Math.round(size * 0.45)}px">${emoji}</span>
    <span style="font-size:${Math.round(size * 0.22)}px;font-weight:700;color:#fff;margin-top:1px;text-shadow:0 1px 2px rgba(0,0,0,0.7)">${sign}${temp}°</span>
    ${countBadge}
  </div>`;
  return L.divIcon({
    html, className: '',
    iconSize:      [size, size + 8],
    iconAnchor:    [size / 2, size / 2],
    tooltipAnchor: [size / 2 + 4, -size / 2],
  });
}

interface Props {
  aircraft: Aircraft;
  onClick?: (a: Aircraft) => void;
}

export default function AircraftMarker({ aircraft, onClick }: Props) {
  const { id, lat, lon, payload } = aircraft;

  // ── Weather station branch ─────────────────────────────────────────────
  const isWeather = payload.__is_weather === true;

  const weatherIcon = useMemo(() => {
    if (!isWeather) return null;
    return makeWeatherIcon(payload.wmo_code ?? 0, payload.temp_c ?? 0, payload.heading, payload.count ?? 1);
  }, [isWeather, payload.wmo_code, payload.temp_c, payload.heading, payload.count]);

  if (isWeather && weatherIcon) {
    const tempC   = payload.temp_c ?? 0;
    const tempCol = getTempColor(tempC);
    const emoji   = WMO_EMOJI[payload.wmo_code ?? 0] ?? '🌡️';
    const feels   = payload.feels_like_c;
    const hum     = payload.humidity_pct;
    const cloud   = payload.cloud_pct;
    const gust    = payload.gust_kt;
    const press   = payload.pressure_hpa;
    const precip  = payload.precip;
    const wdir    = payload.heading;
    const wspd    = payload.velocity;
    const count   = payload.count ?? 1;

    // Compass direction from degrees
    const dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
    const compassDir = wdir != null ? dirs[Math.round(wdir / 22.5) % 16] : null;

    return (
      <Marker
        position={[lat, lon]}
        icon={weatherIcon}
        eventHandlers={{ click: () => onClick?.(aircraft) }}
      >
        <Tooltip sticky direction="top" offset={[0, -4]} opacity={1}>
          <div style={{
            background: '#0f172a', color: '#f1f5f9',
            borderRadius: 8, padding: '10px 14px',
            minWidth: 220, fontSize: 12, lineHeight: 1.8,
            border: `1px solid ${tempCol}55`,
            boxShadow: `0 4px 16px rgba(0,0,0,0.6), 0 0 0 1px ${tempCol}33`,
          }}>
            {/* Header */}
            <div style={{ fontWeight: 700, fontSize: 14, color: '#fff', marginBottom: 2 }}>
              {emoji} {payload.callsign ?? id}
            </div>
            {count > 1 && (
              <div style={{ fontSize: 10, color: '#60a5fa', marginBottom: 2 }}>
                {count} stations · median values
              </div>
            )}
            <div style={{ color: '#94a3b8', fontSize: 11, marginBottom: 8 }}>
              {payload.origin_country ?? '—'}
            </div>

            {/* Temperature row */}
            <div style={{ display: 'flex', gap: 12, marginBottom: 4 }}>
              <div>
                <div style={{ color: '#64748b', fontSize: 10 }}>Temperature</div>
                <div style={{ color: tempCol, fontWeight: 700, fontSize: 15 }}>
                  {Math.round(tempC) > 0 ? '+' : ''}{Math.round(tempC)}°C
                </div>
              </div>
              {feels != null && (
                <div>
                  <div style={{ color: '#64748b', fontSize: 10 }}>Feels like</div>
                  <div style={{ color: '#e2e8f0', fontWeight: 600, fontSize: 13 }}>
                    {Math.round(feels) > 0 ? '+' : ''}{Math.round(feels)}°C
                  </div>
                </div>
              )}
            </div>

            <hr style={{ border: 'none', borderTop: '1px solid rgba(255,255,255,0.08)', margin: '6px 0' }} />

            {/* Stats grid */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '3px 16px' }}>
              <WStat label="Wind"
                value={wspd != null
                  ? `${Math.round(wspd)} kt${compassDir ? ' ' + compassDir : ''}`
                  : '—'} />
              {gust != null && gust > (wspd ?? 0) + 5 && (
                <WStat label="Gusts" value={`${Math.round(gust)} kt`} color="#f97316" />
              )}
              {hum != null   && <WStat label="Humidity"  value={`${Math.round(hum)}%`} />}
              {cloud != null && <WStat label="Cloud"     value={`${Math.round(cloud)}%`} />}
              {precip != null && precip > 0 && (
                <WStat label="Precip" value={`${precip.toFixed(1)} mm/h`} color="#60a5fa" />
              )}
              {press != null && <WStat label="Pressure"  value={`${Math.round(press)} hPa`} />}
            </div>
          </div>
        </Tooltip>
      </Marker>
    );
  }

  // ── Aircraft branch (original) ─────────────────────────────────────────
  const color = getAltitudeColor(payload);
  const type  = getAircraftType(payload);

  const icon = useMemo(
    () => payload.on_ground ? makeGroundIcon() : makePlaneIcon(payload.heading, color),
    [payload.heading, payload.on_ground, color],
  );

  // Build trail from history
  const history = payload.history ?? [];
  const trailPositions: [number, number][] = history.map(h => [h[0], h[1]]);
  // Add current position if not already the last history point
  const lastH = history[history.length - 1];
  if (!lastH || Math.abs(lastH[0] - lat) > 0.0001 || Math.abs(lastH[1] - lon) > 0.0001) {
    trailPositions.push([lat, lon]);
  }

  // Predicted next position (extend the last movement vector by ~30s)
  let predictedPos: [number, number] | null = null;
  if (trailPositions.length >= 2 && !payload.on_ground) {
    const [p1, p2] = trailPositions.slice(-2);
    const dlat = p2[0] - p1[0];
    const dlon = p2[1] - p1[1];
    predictedPos = [lat + dlat * 1.2, lon + dlon * 1.2];
  }

  const callsign = payload.callsign ?? id.toUpperCase();
  const flag     = countryFlag(payload.origin_country);

  return (
    <>
      {/* ── Position trail ────────────────────────────────────────── */}
      {trailPositions.length >= 2 && trailPositions.map((_, i) => {
        if (i === trailPositions.length - 1) return null;
        const seg: [number, number][] = [trailPositions[i], trailPositions[i + 1]];
        const opacity = (i + 1) / (trailPositions.length - 1) * 0.65;
        return (
          <Polyline
            key={`${id}-trail-${i}`}
            positions={seg}
            pathOptions={{ color, weight: 2, opacity, dashArray: i === 0 ? '5,5' : undefined }}
          />
        );
      })}

      {/* ── Predicted direction arrow ─────────────────────────────── */}
      {predictedPos && (
        <Polyline
          key={`${id}-pred`}
          positions={[[lat, lon], predictedPos]}
          pathOptions={{ color, weight: 1.5, opacity: 0.35, dashArray: '3,7' }}
        />
      )}

      {/* ── Marker ───────────────────────────────────────────────── */}
      <Marker
        position={[lat, lon]}
        icon={icon}
        eventHandlers={{ click: () => onClick?.(aircraft) }}
        zIndexOffset={payload.on_ground ? 0 : 100}
      >
        <Tooltip sticky direction="top" offset={[0, -4]} opacity={1} className="">
          <div style={{
            background: '#0f172a', color: '#f1f5f9',
            borderRadius: 8, padding: '10px 13px',
            minWidth: 190, fontSize: 12, lineHeight: 1.7,
            border: `1px solid ${color}55`,
            boxShadow: `0 4px 16px rgba(0,0,0,0.6), 0 0 0 1px ${color}33`,
          }}>
            {/* Header */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
              <img src={getTypeImage(type)} alt={type} width={36} height={36}
                   style={{ filter: 'brightness(1.3)', flexShrink: 0 }} />
              <div>
                <div style={{ fontWeight: 700, fontSize: 14, color: '#fff' }}>{callsign}</div>
                <div style={{ fontSize: 11, color: '#94a3b8' }}>{flag} {payload.origin_country ?? '—'}</div>
              </div>
              <div style={{
                marginLeft: 'auto', background: color + '22', color,
                borderRadius: 4, padding: '2px 7px', fontSize: 10, fontWeight: 600,
              }}>{getTypeLabel(type)}</div>
            </div>

            <hr style={{ border: 'none', borderTop: `1px solid ${color}33`, margin: '6px 0' }} />

            {/* Stats grid */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '3px 16px' }}>
              <Stat label="Altitude" value={fmtAlt(payload.altitude)} color={color} />
              <Stat label="Speed"    value={fmtSpeed(payload.velocity)} color={color} />
              <Stat label="Heading"  value={payload.heading != null ? `${Math.round(payload.heading)}°` : '—'} />
              <Stat label="ICAO24"   value={id.toUpperCase()} />
            </div>

            {/* Trail indicator */}
            {trailPositions.length > 1 && (
              <div style={{ marginTop: 6, fontSize: 10, color: '#64748b' }}>
                📍 {trailPositions.length - 1} position update{trailPositions.length > 2 ? 's' : ''} tracked
                {trailPositions.length >= 2 && (() => {
                  const [p1, p2] = trailPositions.slice(-2);
                  const b = bearing(p1[0], p1[1], p2[0], p2[1]);
                  const dirs = ['N','NE','E','SE','S','SW','W','NW'];
                  const dir = dirs[Math.round(b / 45) % 8];
                  return ` · heading ${dir}`;
                })()}
              </div>
            )}
          </div>
        </Tooltip>
      </Marker>
    </>
  );
}

function Stat({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <span style={{ color: '#64748b', fontSize: 10 }}>{label}</span>
      <div style={{ color: color ?? '#e2e8f0', fontWeight: 600, fontSize: 12 }}>{value}</div>
    </div>
  );
}

function WStat({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <span style={{ color: '#64748b', fontSize: 10 }}>{label}</span>
      <div style={{ color: color ?? '#e2e8f0', fontWeight: 600, fontSize: 12 }}>{value}</div>
    </div>
  );
}
