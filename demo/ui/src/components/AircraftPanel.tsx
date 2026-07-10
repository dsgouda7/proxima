import { useState, useRef } from 'react';
import { Aircraft } from '../types';
import {
  getAircraftType, getAltitudeColor, getTypeImage, getTypeLabel,
  fmtAlt, fmtSpeed, countryFlag,
} from '../utils/aircraft';

interface Props {
  aircraft: Aircraft[];
  onSelect: (a: Aircraft) => void;
  selected: string | null;
}

export default function AircraftPanel({ aircraft, onSelect, selected }: Props) {
  const [open,   setOpen]   = useState(true);
  const [filter, setFilter] = useState('');
  const listRef = useRef<HTMLDivElement>(null);

  // Sort: airborne first by altitude desc, then ground alphabetically
  const sorted = [...aircraft].sort((a, b) => {
    const aAlt = a.payload.altitude ?? -1;
    const bAlt = b.payload.altitude ?? -1;
    if (a.payload.on_ground !== b.payload.on_ground) {
      return a.payload.on_ground ? 1 : -1;
    }
    return bAlt - aAlt;
  });

  const query = filter.toLowerCase();
  const filtered = query
    ? sorted.filter(a =>
        (a.payload.callsign ?? a.id).toLowerCase().includes(query) ||
        (a.payload.origin_country ?? '').toLowerCase().includes(query),
      )
    : sorted;

  const displayed = filtered.slice(0, 120);

  const airborne = aircraft.filter(a => !a.payload.on_ground).length;
  const ground   = aircraft.length - airborne;

  return (
    <div style={{
      position: 'absolute', top: 8, right: 8, zIndex: 1000,
      width: open ? 280 : 48,
      background: 'rgba(15,23,42,0.95)',
      border: '1px solid rgba(255,255,255,0.08)',
      borderRadius: 10,
      backdropFilter: 'blur(8px)',
      transition: 'width 0.2s ease',
      overflow: 'hidden',
      display: 'flex',
      flexDirection: 'column',
      maxHeight: 'calc(100vh - 80px)',
      boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
    }}>

      {/* Toggle button */}
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          background: 'none', border: 'none', cursor: 'pointer',
          color: '#94a3b8', padding: '10px 14px',
          display: 'flex', alignItems: 'center', gap: 8,
          fontSize: 13, fontWeight: 600, flexShrink: 0,
          textAlign: 'left', width: '100%',
        }}
      >
        <span style={{ fontSize: 18 }}>✈</span>
        {open && (
          <>
            <span style={{ color: '#e2e8f0' }}>Live Flights</span>
            <span style={{
              marginLeft: 'auto',
              background: '#0ea5e9', color: '#fff',
              borderRadius: 10, padding: '1px 8px', fontSize: 11,
            }}>{aircraft.length.toLocaleString()}</span>
          </>
        )}
      </button>

      {open && (
        <>
          {/* Stats row */}
          <div style={{
            display: 'flex', gap: 0, borderTop: '1px solid rgba(255,255,255,0.06)',
            borderBottom: '1px solid rgba(255,255,255,0.06)',
          }}>
            <StatChip icon="🛫" label="Airborne" value={airborne.toLocaleString()} color="#38bdf8" />
            <StatChip icon="🛬" label="Ground"   value={ground.toLocaleString()}   color="#64748b" />
          </div>

          {/* Search */}
          <div style={{ padding: '8px 10px', flexShrink: 0 }}>
            <input
              value={filter}
              onChange={e => setFilter(e.target.value)}
              placeholder="Search callsign / country…"
              style={{
                width: '100%', background: 'rgba(255,255,255,0.07)',
                border: '1px solid rgba(255,255,255,0.1)', borderRadius: 6,
                color: '#e2e8f0', padding: '5px 10px', fontSize: 12,
                outline: 'none', boxSizing: 'border-box',
              }}
            />
          </div>

          {/* List */}
          <div ref={listRef} style={{ overflowY: 'auto', flex: 1 }}>
            {displayed.map(a => (
              <AircraftRow
                key={a.id}
                aircraft={a}
                selected={selected === a.id}
                onClick={onSelect}
              />
            ))}
            {filtered.length > 120 && (
              <div style={{ color: '#475569', fontSize: 11, textAlign: 'center', padding: '8px 0' }}>
                +{filtered.length - 120} more — refine search
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// ── Row ──────────────────────────────────────────────────────────────────────

function AircraftRow({
  aircraft: a, selected, onClick,
}: { aircraft: Aircraft; selected: boolean; onClick: (a: Aircraft) => void }) {
  const type  = getAircraftType(a.payload);
  const color = getAltitudeColor(a.payload);
  const flag  = countryFlag(a.payload.origin_country);

  return (
    <div
      onClick={() => onClick(a)}
      style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '6px 10px', cursor: 'pointer',
        background: selected ? `${color}18` : 'transparent',
        borderLeft: selected ? `3px solid ${color}` : '3px solid transparent',
        transition: 'background 0.15s',
      }}
      onMouseEnter={e => { if (!selected) (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,0.04)'; }}
      onMouseLeave={e => { if (!selected) (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
    >
      {/* Aircraft type thumbnail */}
      <img
        src={getTypeImage(type)}
        alt={type}
        width={30}
        height={30}
        style={{ flexShrink: 0, opacity: a.payload.on_ground ? 0.5 : 1 }}
      />

      {/* Info */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{
          display: 'flex', alignItems: 'center', gap: 4,
          fontWeight: 700, fontSize: 12, color: '#f1f5f9',
        }}>
          <span>{flag}</span>
          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {a.payload.callsign ?? a.id.toUpperCase()}
          </span>
          {a.payload.on_ground && (
            <span style={{ marginLeft: 'auto', color: '#64748b', fontSize: 10, flexShrink: 0 }}>GND</span>
          )}
        </div>
        <div style={{ display: 'flex', gap: 8, fontSize: 10, color: '#64748b', marginTop: 1 }}>
          <span style={{ color }}>{fmtAlt(a.payload.altitude)}</span>
          <span>{fmtSpeed(a.payload.velocity)}</span>
          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
            {getTypeLabel(type)}
          </span>
        </div>
      </div>

      {/* Trail dots */}
      {(a.payload.history?.length ?? 0) > 1 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, flexShrink: 0 }}>
          {[...Array(Math.min(a.payload.history!.length, 3))].map((_, i) => (
            <div key={i} style={{
              width: 4, height: 4, borderRadius: '50%',
              background: color,
              opacity: (i + 1) / 3,
            }} />
          ))}
        </div>
      )}
    </div>
  );
}

function StatChip({ icon, label, value, color }: { icon: string; label: string; value: string; color: string }) {
  return (
    <div style={{
      flex: 1, padding: '6px 10px', display: 'flex', flexDirection: 'column',
      alignItems: 'center', borderRight: '1px solid rgba(255,255,255,0.06)',
    }}>
      <div style={{ fontSize: 11, color: '#475569' }}>{icon} {label}</div>
      <div style={{ fontSize: 14, fontWeight: 700, color }}>{value}</div>
    </div>
  );
}
