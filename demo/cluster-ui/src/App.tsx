import { useCallback } from 'react';
import { useCluster } from './hooks/useCluster';
import Topology      from './components/Topology';
import { ThroughputChart, KeyDistribution } from './components/Charts';
import EventLog      from './components/EventLog';
import ControlPanel  from './components/ControlPanel';
import WeatherPanel  from './components/WeatherPanel';
import type { ClusterEvent } from './types';
import { STATUS_COLOR } from './types';
import { CLUSTER_NODES } from './config';

const card: React.CSSProperties = {
  background:   '#0a1628',
  border:       '1px solid #1e3a5f',
  borderRadius: 10,
  padding:      '14px 18px',
};

export default function App() {
  const { current, snapshots, throughput, events, reachable } = useCluster();
  const nodes = current?.nodes ?? [];

  // Determine current phase from node statuses
  const phase = (() => {
    if (!reachable)                          return 'OFFLINE';
    if (nodes.some(n => n.status === 'splitting'))     return 'SPLITTING';
    if (nodes.some(n => n.status === 'bootstrapping')) return 'BOOTSTRAPPING';
    if (nodes.some(n => n.status === 'merging'))       return 'MERGING';
    if (nodes.some(n => n.status === 'suspect'))       return 'DEGRADED';
    if (nodes.filter(n => n.status === 'active').length >= 3) return 'HEALTHY';
    return 'STARTING';
  })();

  const phaseColor = {
    OFFLINE: '#ef4444', SPLITTING: '#eab308', BOOTSTRAPPING: '#3b82f6',
    MERGING: '#06b6d4', DEGRADED: '#f97316', HEALTHY: '#22c55e', STARTING: '#94a3b8',
  }[phase];

  const totalKeys = nodes.reduce((s, n) => s + n.key_count, 0);
  const wps = (() => {
    if (throughput.length < 2) return 0;
    const recent = throughput.slice(-5);
    const dt = (recent[recent.length-1].ts - recent[0].ts) / 1000;
    const dk = recent[recent.length-1].total - recent[0].total;
    return dt > 0 ? Math.round(dk / dt) : 0;
  })();

  // Allow ControlPanel to inject events
  const addExternalEvent = useCallback((msg: string, kind: ClusterEvent['kind']) => {
    // Events come from the polling hook; ControlPanel fires them via this callback
  }, []);

  return (
    <div style={{ minHeight: '100vh', padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>

      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700, color: '#38bdf8', letterSpacing: 0.5 }}>
            geo-redis
          </h1>
          <div style={{ fontSize: 12, color: '#475569' }}>Cluster Monitor</div>
        </div>

        {/* Phase badge */}
        <div style={{
          padding: '4px 14px', borderRadius: 20,
          background: phaseColor + '22', border: `1px solid ${phaseColor}`,
          color: phaseColor, fontSize: 12, fontWeight: 700, letterSpacing: 1,
        }}>
          {phase}
        </div>

        {/* Quick stats */}
        <div style={{ display: 'flex', gap: 20, marginLeft: 'auto', flexWrap: 'wrap' }}>
          {[
            { label: 'Nodes',      value: `${nodes.filter(n => n.status === 'active').length} / ${nodes.length}` },
            { label: 'Total keys', value: totalKeys.toLocaleString() },
            { label: 'Rate',       value: `${wps.toLocaleString()} k/s` },
          ].map(({ label, value }) => (
            <div key={label} style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 10, color: '#475569' }}>{label}</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: '#e2e8f0', fontFamily: 'monospace' }}>
                {value}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Control bar ────────────────────────────────────────────────────── */}
      <ControlPanel reachable={reachable} onEvent={addExternalEvent} nodes={nodes} />

      {/* ── Main grid ──────────────────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 12, flex: 1 }}>

        {/* Left column: topology + charts */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

          {/* Topology */}
          <div style={{ ...card }}>
            <div style={{ color: '#64748b', fontSize: 11, marginBottom: 8 }}>
              Cluster topology — S2 token ring partitioning
            </div>
            <Topology nodes={nodes} />
          </div>

          {/* Charts row */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div style={{ ...card }}>
              <ThroughputChart data={throughput} />
            </div>
            <div style={{ ...card }}>
              <KeyDistribution snapshot={current} />
            </div>
          </div>
        </div>

        {/* Right column: node cards + event log */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

          {/* Node cards */}
          <div style={{ ...card }}>
            <div style={{ color: '#64748b', fontSize: 11, marginBottom: 10 }}>Node status</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {nodes.length === 0
                ? <div style={{ color: '#334155', fontSize: 12 }}>No nodes — start the cluster</div>
                : nodes.map(n => <NodeCard key={n.node_id} node={n} />)
              }
            </div>
          </div>

          {/* Event log */}
          <div style={{ ...card, flex: 1, overflow: 'hidden' }}>
            <div style={{ color: '#64748b', fontSize: 11, marginBottom: 10 }}>
              Cluster events
              <span style={{ float: 'right', color: '#334155' }}>{events.length} total</span>
            </div>
            <div style={{ maxHeight: 300, overflowY: 'auto' }}>
              <EventLog events={events} />
            </div>
          </div>

          {/* Weather stream panel */}
          <WeatherPanel />
        </div>
      </div>

      {/* ── Footer ─────────────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 20, color: '#334155', fontSize: 10 }}>
        <span>Polls every 2 s · Nodes {CLUSTER_NODES.map(n => new URL(n).port).join(', ')}</span>
        <span>{snapshots.length} snapshots captured</span>
        <span style={{ marginLeft: 'auto' }}>
          Start cluster: <code style={{ color: '#475569' }}>docker compose -f demo/cluster-compose.yml up -d</code>
        </span>
      </div>
    </div>
  );
}

// ── Node card ──────────────────────────────────────────────────────────────

function NodeCard({ node }: { node: import('./types').NodeInfo }) {
  const col  = STATUS_COLOR[node.status];
  const pfxS = node.prefix_start || '∅';
  const pfxE = node.prefix_end   || '∅';
  const memMb = (node.mem_bytes / 1_048_576).toFixed(1);

  return (
    <div style={{
      padding: '8px 10px',
      borderRadius: 8,
      border: `1px solid ${col}33`,
      background: col + '0a',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <div style={{ width: 8, height: 8, borderRadius: '50%', background: col, flexShrink: 0 }} />
        <span style={{ fontWeight: 700, fontSize: 13, color: '#e2e8f0', flex: 1 }}>
          {node.node_id}
        </span>
        <span style={{
          fontSize: 10, padding: '1px 7px', borderRadius: 10,
          background: col + '33', color: col, fontWeight: 600,
        }}>
          {node.status}
        </span>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 4, marginTop: 8 }}>
        {[
          { k: 'Keys',   v: node.key_count.toLocaleString() },
          { k: 'Memory', v: `${memMb} MB` },
          { k: 'Range',  v: `[${pfxS}, ${pfxE})` },
          { k: 'Gen',    v: `#${node.generation}` },
        ].map(({ k, v }) => (
          <div key={k}>
            <div style={{ fontSize: 9,  color: '#475569' }}>{k}</div>
            <div style={{ fontSize: 11, color: '#94a3b8', fontFamily: 'monospace' }}>{v}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
