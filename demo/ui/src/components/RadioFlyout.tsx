import { useState, useRef, useEffect, useCallback } from 'react';
import { Aircraft, RadioStationInfo } from '../types';

// Country code → emoji flag (regional indicator letters).
function countryFlag(cc: string): string {
  if (!cc || cc.length !== 2) return '';
  return cc
    .toUpperCase()
    .replace(/./g, c => String.fromCodePoint(c.charCodeAt(0) + 0x1f1a5));
}

// ── Sub-components ─────────────────────────────────────────────────────────

function StationRow({
  station,
  isPlaying,
  isLoading,
  hasError,
  onToggle,
}: {
  station: RadioStationInfo;
  isPlaying: boolean;
  isLoading: boolean;
  hasError: boolean;
  onToggle: (s: RadioStationInfo) => void;
}) {
  const flag = countryFlag(station.countrycode);
  const tags = station.tags
    ? station.tags.split(',').slice(0, 3).map(t => t.trim()).filter(Boolean)
    : [];

  return (
    <div
      style={{
        display: 'flex', alignItems: 'center', gap: 10,
        padding: '9px 12px',
        background: isPlaying ? 'rgba(99,102,241,0.12)' : 'transparent',
        borderBottom: '1px solid rgba(255,255,255,0.05)',
        borderLeft: isPlaying ? '3px solid #6366f1' : '3px solid transparent',
        cursor: 'pointer',
        transition: 'background 0.15s',
      }}
      onMouseEnter={e => { if (!isPlaying) (e.currentTarget as HTMLDivElement).style.background = 'rgba(255,255,255,0.04)'; }}
      onMouseLeave={e => { if (!isPlaying) (e.currentTarget as HTMLDivElement).style.background = 'transparent'; }}
      onClick={() => onToggle(station)}
    >
      {/* Favicon */}
      <div style={{ flexShrink: 0, width: 36, height: 36 }}>
        {station.favicon ? (
          <img
            src={station.favicon}
            alt=""
            width={36} height={36}
            style={{ borderRadius: 6, objectFit: 'contain', background: '#1e293b' }}
            onError={e => { (e.target as HTMLImageElement).style.display = 'none'; }}
          />
        ) : (
          <div style={{
            width: 36, height: 36, borderRadius: 6,
            background: '#1e293b', display: 'flex', alignItems: 'center',
            justifyContent: 'center', fontSize: 18,
          }}>📻</div>
        )}
      </div>

      {/* Info */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{
          fontWeight: 600, fontSize: 12, color: isPlaying ? '#a5b4fc' : '#e2e8f0',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {station.name}
        </div>
        {tags.length > 0 && (
          <div style={{ fontSize: 10, color: '#64748b', marginTop: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {tags.join(' · ')}
          </div>
        )}
        <div style={{ fontSize: 10, color: '#475569', marginTop: 1, display: 'flex', gap: 6 }}>
          {flag && <span>{flag}</span>}
          {station.bitrate > 0 && <span>{station.bitrate} kbps</span>}
          {station.codec && <span>{station.codec}</span>}
        </div>
      </div>

      {/* Play / pause button */}
      <button
        style={{
          flexShrink: 0,
          width: 32, height: 32,
          borderRadius: '50%',
          border: isPlaying ? '1.5px solid #6366f1' : '1.5px solid rgba(129,140,248,0.4)',
          background: isPlaying ? 'rgba(99,102,241,0.25)' : 'rgba(255,255,255,0.05)',
          color: isPlaying ? '#a5b4fc' : '#94a3b8',
          cursor: 'pointer',
          fontSize: 13,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          transition: 'all 0.15s',
        }}
        title={isPlaying ? 'Stop' : 'Play'}
        onClick={e => { e.stopPropagation(); onToggle(station); }}
      >
        {isLoading ? '…' : isPlaying ? '⏹' : '▶'}
      </button>

      {hasError && (
        <span style={{ fontSize: 9, color: '#f87171', marginLeft: 2 }} title="Stream unreachable">⚠</span>
      )}
    </div>
  );
}

// ── Main flyout panel ──────────────────────────────────────────────────────

interface Props {
  cluster: Aircraft;
  onClose: () => void;
}

export default function RadioFlyout({ cluster, onClose }: Props) {
  const stations: RadioStationInfo[] = cluster.payload.stations ?? [];
  const cellName = cluster.payload.callsign ?? `${stations.length} stations`;

  const [playing, setPlaying]     = useState<string | null>(null);
  const [loading, setLoading]     = useState<string | null>(null);
  const [error, setError]         = useState<string | null>(null);
  const [errorId, setErrorId]     = useState<string | null>(null);
  const [search, setSearch]       = useState('');
  const audioRef                  = useRef<HTMLAudioElement | null>(null);

  // Stop audio when flyout closes or cluster changes.
  useEffect(() => {
    return () => {
      if (audioRef.current) {
        audioRef.current.pause();
        audioRef.current.src = '';
        audioRef.current = null;
      }
    };
  }, [cluster.id]);

  const handleToggle = useCallback((station: RadioStationInfo) => {
    // Stop whatever is playing.
    if (audioRef.current) {
      audioRef.current.pause();
      audioRef.current.src = '';
      audioRef.current = null;
    }
    setError(null);
    setErrorId(null);

    if (playing === station.uuid) {
      setPlaying(null);
      setLoading(null);
      return;
    }

    setLoading(station.uuid);
    const audio = new Audio(station.stream_url);

    audio.addEventListener('canplay', () => {
      setLoading(null);
      setPlaying(station.uuid);
    }, { once: true });

    audio.onerror = () => {
      setLoading(null);
      setPlaying(null);
      setError(`Can't connect to "${station.name}"`);
      setErrorId(station.uuid);
      audioRef.current = null;
    };

    // Start playing — some streams start immediately, others buffer first.
    audio.play().catch(() => {
      // error handled via onerror
    });

    audioRef.current = audio;
    // Optimistic: mark as playing immediately (loading indicator shows)
    setPlaying(station.uuid);
  }, [playing]);

  const filtered = search.trim()
    ? stations.filter(s =>
        s.name.toLowerCase().includes(search.toLowerCase()) ||
        s.tags.toLowerCase().includes(search.toLowerCase()) ||
        s.country.toLowerCase().includes(search.toLowerCase()),
      )
    : stations;

  const flag = countryFlag(cluster.payload.top_cc ?? '');

  return (
    <div style={{
      position: 'absolute', top: 0, right: 0, bottom: 0,
      width: 320, zIndex: 1200,
      background: 'rgba(10,15,30,0.97)',
      borderLeft: '1px solid rgba(129,140,248,0.2)',
      backdropFilter: 'blur(8px)',
      display: 'flex', flexDirection: 'column',
      boxShadow: '-8px 0 32px rgba(0,0,0,0.5)',
    }}>
      {/* Header */}
      <div style={{
        padding: '12px 14px 10px',
        borderBottom: '1px solid rgba(255,255,255,0.07)',
        background: 'linear-gradient(180deg, rgba(99,102,241,0.12), transparent)',
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 20 }}>📻</span>
            <div>
              <div style={{ fontWeight: 700, fontSize: 13, color: '#e2e8f0' }}>{cellName}</div>
              {flag && (
                <div style={{ fontSize: 11, color: '#64748b' }}>
                  {flag} {cluster.payload.origin_country}
                </div>
              )}
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'none', border: 'none',
              color: '#64748b', cursor: 'pointer', fontSize: 18, lineHeight: 1,
              padding: '2px 4px', borderRadius: 4,
            }}
            title="Close"
          >✕</button>
        </div>

        {/* Playing indicator */}
        {playing && (
          <div style={{
            marginTop: 8, fontSize: 10, color: '#818cf8',
            display: 'flex', alignItems: 'center', gap: 4,
          }}>
            <span style={{ animation: 'pulse 1s infinite' }}>▶</span>
            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {stations.find(s => s.uuid === playing)?.name ?? 'Playing'}
            </span>
          </div>
        )}

        {/* Error banner */}
        {error && (
          <div style={{
            marginTop: 6, fontSize: 10, color: '#f87171',
            background: 'rgba(248,113,113,0.1)', borderRadius: 4,
            padding: '3px 6px',
          }}>
            ⚠ {error}
          </div>
        )}

        {/* Search */}
        <input
          type="text"
          placeholder="Search station, genre, country…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{
            width: '100%', marginTop: 8,
            background: 'rgba(255,255,255,0.05)',
            border: '1px solid rgba(255,255,255,0.1)',
            borderRadius: 6, padding: '5px 9px',
            color: '#e2e8f0', fontSize: 11,
            outline: 'none', boxSizing: 'border-box',
          }}
        />
      </div>

      {/* Station list */}
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {filtered.length === 0 ? (
          <div style={{ padding: 16, color: '#475569', fontSize: 12, textAlign: 'center' }}>
            {search ? 'No stations match that search' : 'No stations in this area'}
          </div>
        ) : (
          filtered.map(s => (
            <StationRow
              key={s.uuid}
              station={s}
              isPlaying={playing === s.uuid}
              isLoading={loading === s.uuid && playing !== s.uuid}
              hasError={errorId === s.uuid}
              onToggle={handleToggle}
            />
          ))
        )}
      </div>

      {/* Footer */}
      <div style={{
        padding: '7px 12px',
        borderTop: '1px solid rgba(255,255,255,0.05)',
        fontSize: 9, color: '#334155', flexShrink: 0,
      }}>
        {filtered.length} / {stations.length} stations
        {' · '}data: <a href="https://www.radio-browser.info" target="_blank" rel="noreferrer"
          style={{ color: '#4b5563' }}>Radio Browser</a>
      </div>
    </div>
  );
}
