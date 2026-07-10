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

interface Props {
  aircraft: Aircraft;
  onClick?: (a: Aircraft) => void;
}

export default function AircraftMarker({ aircraft, onClick }: Props) {
  const { id, lat, lon, payload } = aircraft;
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
