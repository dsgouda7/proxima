import { useWeather, getWmoEmoji } from '../hooks/useWeather';

const tempColor = (t: number) =>
  t >= 35 ? '#ef4444' : t >= 25 ? '#f97316' : t >= 15 ? '#eab308' :
  t >= 5  ? '#22c55e' : t >= -5 ? '#06b6d4' : '#3b82f6';

export default function WeatherPanel() {
  const { metrics, events, streaming, reachable } = useWeather();

  const syncTime = metrics?.last_sync
    ? new Date(metrics.last_sync * 1000).toLocaleTimeString()
    : null;

  const pct = streaming ? Math.round((streaming.n / streaming.total) * 100) : 0;

  return (
    <div style={{
      background:   '#0a1628',
      border:       '1px solid #1e3a5f',
      borderRadius: 10,
      padding:      '14px 18px',
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 }}>
        <div style={{
          width: 8, height: 8, borderRadius: '50%', flexShrink: 0,
          background: reachable ? '#22c55e' : '#475569',
          boxShadow:  reachable ? '0 0 6px #22c55e' : undefined,
        }} />
        <span style={{ fontWeight: 700, fontSize: 13, color: '#e2e8f0', flex: 1 }}>
          Weather Stream
        </span>
        {reachable && (
          <span style={{ fontSize: 10, color: '#475569', fontFamily: 'monospace' }}>
            :3001
          </span>
        )}
        {!reachable && (
          <span style={{ fontSize: 10, color: '#475569' }}>
            start weather-server first
          </span>
        )}
      </div>

      {reachable && metrics && (
        <>
          {/* Stats row */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 8, marginBottom: 12 }}>
            <Stat label="Stations" value={metrics.trie_size.toLocaleString()}
              highlight={!!streaming} />
            <Stat label="Writes"   value={metrics.metrics.write_count.toLocaleString()} />
            <Stat label="Synced"   value={syncTime ?? '—'} />
          </div>

          {/* Source tag */}
          <div style={{
            fontSize: 9, color: '#334155', fontFamily: 'monospace',
            marginBottom: streaming ? 10 : 0,
            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>
            {metrics.source}
          </div>

          {/* Streaming progress */}
          {streaming && (
            <div style={{ marginBottom: 12 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                <span style={{ fontSize: 10, color: '#3b82f6' }}>
                  ⚡ Streaming METAR events
                </span>
                <span style={{ fontSize: 10, color: '#64748b', fontFamily: 'monospace' }}>
                  {streaming.n.toLocaleString()} / {streaming.total.toLocaleString()}
                </span>
              </div>
              <div style={{ background: '#0f172a', borderRadius: 4, height: 6, overflow: 'hidden' }}>
                <div style={{
                  height: '100%',
                  width:  `${pct}%`,
                  background: 'linear-gradient(90deg,#3b82f6,#818cf8)',
                  borderRadius: 4,
                  transition: 'width 0.2s ease',
                }} />
              </div>
            </div>
          )}

          {/* Live event ticker */}
          {events.length > 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              <div style={{ fontSize: 10, color: '#334155', marginBottom: 4 }}>
                Live station feed
              </div>
              {[...events].reverse().slice(0, 6).map((e, i) => (
                <div key={`${e.id}-${i}`} style={{
                  display:    'flex',
                  alignItems: 'center',
                  gap:        6,
                  padding:    '3px 6px',
                  borderRadius: 4,
                  background: i === 0 ? '#0f1f3a' : 'transparent',
                  transition: 'background 0.3s',
                }}>
                  <span style={{ fontSize: 13 }}>{getWmoEmoji(e.wmo_code)}</span>
                  <span style={{
                    width: 52, fontFamily: 'monospace', fontSize: 10,
                    color: '#94a3b8', overflow: 'hidden', textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap', flexShrink: 0,
                  }}>{e.id}</span>
                  <span style={{
                    fontSize: 11, fontWeight: 700, fontFamily: 'monospace',
                    color: tempColor(e.temp_c), flexShrink: 0, width: 40, textAlign: 'right',
                  }}>
                    {e.temp_c > 0 ? '+' : ''}{Math.round(e.temp_c)}°C
                  </span>
                  <span style={{
                    fontSize: 9, color: '#475569', flex: 1,
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                  }}>{e.condition || '—'}</span>
                  <span style={{ fontSize: 9, color: '#1e3a5f', fontFamily: 'monospace', flexShrink: 0 }}>
                    {e.lat.toFixed(1)},{e.lon.toFixed(1)}
                  </span>
                </div>
              ))}
            </div>
          )}

          {events.length === 0 && (
            <div style={{ fontSize: 11, color: '#334155' }}>
              Waiting for next METAR cycle…
            </div>
          )}
        </>
      )}

      {!reachable && (
        <div style={{ fontSize: 11, color: '#334155' }}>
          <div style={{ marginBottom: 6 }}>Start the weather server to see live METAR events:</div>
          <code style={{
            display: 'block', fontSize: 9, color: '#475569',
            background: '#020617', padding: '6px 8px', borderRadius: 4,
            fontFamily: 'monospace',
          }}>
            cargo run --release -p georedis-weather
          </code>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div>
      <div style={{ fontSize: 9, color: '#475569' }}>{label}</div>
      <div style={{
        fontSize: 13, fontWeight: 700, fontFamily: 'monospace',
        color: highlight ? '#3b82f6' : '#94a3b8',
        transition: 'color 0.3s',
      }}>{value}</div>
    </div>
  );
}
