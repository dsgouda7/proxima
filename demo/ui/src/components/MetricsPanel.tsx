import { MetricsResponse } from '../api/client';

const ms = (us: number) => `${(us / 1000).toFixed(2)} ms`;

interface Props {
  metrics: MetricsResponse;
  entityLabel?: string;
  /** Panel heading; defaults to 'Redis Metrics'. */
  title?: string;
  /** When true, hides the write/read latency rows (e.g. for in-memory demos). */
  hideLatency?: boolean;
}

export default function MetricsPanel({ metrics: { metrics: m, trie_size, last_sync }, entityLabel = 'Aircraft', title = 'Redis Metrics', hideLatency = false }: Props) {
  const syncTime = last_sync
    ? new Date(last_sync * 1000).toLocaleTimeString()
    : 'pending…';

  return (
    <div
      style={{
        position:   'absolute',
        bottom:     24,
        right:      12,
        zIndex:     1000,
        background: 'rgba(15,23,42,0.92)',
        color:      '#e2e8f0',
        padding:    '12px 16px',
        borderRadius: 8,
        minWidth:   220,
        fontFamily: 'monospace',
        fontSize:   '0.76rem',
        lineHeight: 1.9,
        backdropFilter: 'blur(4px)',
      }}
    >
      <div style={{ fontWeight: 700, fontSize: '0.82rem', marginBottom: 4 }}>
        {title}
      </div>
      <div>{entityLabel} in index: <b>{trie_size.toLocaleString()}</b></div>
      <div>Last sync: <b>{syncTime}</b></div>

      <hr style={{ border: 'none', borderTop: '1px solid #334155', margin: '6px 0' }} />

      {!hideLatency && (
        <>
          <div style={{ color: '#7dd3fc' }}>Writes ({m.write_count.toLocaleString()})</div>
          <div>avg {ms(m.write_avg_us)} · max {ms(m.write_max_us)}</div>

          <div style={{ color: '#6ee7b7', marginTop: 4 }}>Reads ({m.read_count.toLocaleString()})</div>
          <div>avg {ms(m.read_avg_us)} · max {ms(m.read_max_us)}</div>
        </>
      )}

      {(m.nearby_count ?? 0) > 0 && (
        <>
          <div style={{ color: '#f9a8d4', marginTop: 4 }}>Nearby ({m.nearby_count!.toLocaleString()})</div>
          <div>avg {ms(m.nearby_avg_us ?? 0)} · max {ms(m.nearby_max_us ?? 0)}</div>
        </>
      )}
    </div>
  );
}
