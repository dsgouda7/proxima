import { useMemo } from 'react';
import { Marker, Tooltip } from 'react-leaflet';
import L from 'leaflet';
import { Aircraft } from '../types';

// ── Icon factory ──────────────────────────────────────────────────────────

function makeRadioIcon(count: number, isLeaf: boolean): L.DivIcon {
  // Scale size logarithmically: 36px for 1 station, up to 58px for 1 000+.
  const size   = Math.min(58, 36 + Math.floor(Math.log2(count + 1) * 3));
  const half   = size / 2;

  const border   = isLeaf
    ? '2px solid rgba(129,140,248,0.95)'   // indigo glow — leaf/interactive
    : '2px solid rgba(168,85,247,0.55)';   // purple dim — non-leaf

  const cursor = isLeaf ? 'pointer' : 'default';
  const glow   = isLeaf
    ? 'box-shadow:0 0 0 3px rgba(129,140,248,0.18),0 2px 8px rgba(0,0,0,0.6);'
    : 'box-shadow:0 2px 6px rgba(0,0,0,0.5);';

  const badge = count > 1
    ? `<div style="
          position:absolute;bottom:-7px;left:50%;transform:translateX(-50%);
          background:#0f172a;color:#a5b4fc;font-size:7px;font-weight:700;
          padding:1px 5px;border-radius:6px;white-space:nowrap;
          border:1px solid rgba(129,140,248,0.35)
        ">${count >= 1000 ? `${(count / 1000).toFixed(1)}k` : count}</div>`
    : '';

  const html = `
    <div style="
      position:relative;width:${size}px;height:${size}px;
      background:radial-gradient(circle at 35% 35%,#1e1b4b,#0f172a);
      border-radius:50%;border:${border};${glow}
      display:flex;align-items:center;justify-content:center;
      cursor:${cursor};
    ">
      <span style="font-size:${Math.round(size * 0.42)}px;line-height:1;user-select:none">📻</span>
      ${badge}
    </div>`;

  return L.divIcon({
    html,
    className:   '',
    iconSize:    [size, size + 8],
    iconAnchor:  [half, half],
    tooltipAnchor: [half + 4, -half],
  });
}

// ── Component ──────────────────────────────────────────────────────────────

interface Props {
  cluster:  Aircraft;
  isLeaf:   boolean;
  onClick?: (a: Aircraft) => void;
}

export default function RadioMarker({ cluster, isLeaf, onClick }: Props) {
  const { lat, lon, payload } = cluster;
  const count = payload.count ?? 1;

  const icon = useMemo(
    () => makeRadioIcon(count, isLeaf),
    [count, isLeaf],
  );

  const topTags = payload.top_tags?.split(',').slice(0, 3).join(' · ') ?? '';

  return (
    <Marker
      position={[lat, lon]}
      icon={icon}
      eventHandlers={isLeaf && onClick ? { click: () => onClick(cluster) } : {}}
    >
      <Tooltip direction="top" offset={[0, -4]} opacity={1}>
        <div style={{
          background: '#0f172a', color: '#f1f5f9',
          borderRadius: 8, padding: '8px 12px',
          minWidth: 160, fontSize: 12, lineHeight: 1.7,
          border: '1px solid rgba(129,140,248,0.3)',
          boxShadow: '0 4px 16px rgba(0,0,0,0.6)',
        }}>
          <div style={{ fontWeight: 700, fontSize: 13 }}>
            📻 {count === 1 ? payload.callsign : `${count} stations`}
          </div>
          {topTags && (
            <div style={{ color: '#a5b4fc', fontSize: 10, marginTop: 2 }}>
              {topTags}
            </div>
          )}
          <div style={{ color: '#64748b', fontSize: 10, marginTop: 4 }}>
            {isLeaf ? '▶  Click to browse channels' : '🔍 Zoom in to browse channels'}
          </div>
        </div>
      </Tooltip>
    </Marker>
  );
}
