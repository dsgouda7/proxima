import { useEffect, useMemo, useState } from 'react';
import { fetchTrieSnapshot } from '../api/client';
import type { TrieSnapshot } from '../types';
import './TrieExplorer.css';

const POLL_INTERVAL_MS = 5000;

/** One step along the root→leaf path for the highlighted entry's S2 token. */
interface PathStep {
  prefix: string; // hex prefix at this depth ('' = root)
  count:  number; // entries anywhere under this prefix
}

/**
 * Computes entry counts along the ancestor chain of `leafToken`, purely from
 * the flat {id, token} list the backend already exposes. Each character of
 * an S2 token is one trie level — this mirrors exactly how `GeoTrie::insert`
 * descends byte-by-byte in lib/src/trie.rs — so no new introspection method
 * is needed on the library itself, and no full tree needs to be built: we
 * only ever show the single path relevant to the hovered/selected entry.
 */
function computePath(nodes: { token: string }[], leafToken: string): PathStep[] {
  const steps: PathStep[] = [];
  for (let i = 0; i <= leafToken.length; i++) {
    const prefix = leafToken.slice(0, i);
    let count = 0;
    for (const n of nodes) if (n.token.startsWith(prefix)) count++;
    steps.push({ prefix, count });
  }
  return steps;
}

interface Props {
  /** id of the aircraft/station currently hovered or selected on the map. */
  highlightId?: string | null;
}

/**
 * On-demand view of the S2 trie's root→leaf path for whichever entry is
 * currently hovered/selected on the map — not a browsable tree, just the
 * chain of prefixes that led to that entry. Renders nothing at all until a
 * marker is hovered or clicked; no always-on panel, no background polling
 * while idle, and no scrollbars — the path is always short enough (token
 * length + 1 steps) to fit in a single wrapped row.
 *
 * Polls GET /api/trie only while `highlightId` is set. The trie's internal
 * shape is fully derivable from S2 token strings alone, so no geo-redis
 * (lib/) changes were needed.
 */
export default function TrieExplorer({ highlightId }: Props) {
  const [snapshot, setSnapshot] = useState<TrieSnapshot | null>(null);
  const [error, setError] = useState(false);

  // Only fetch/poll while something is actually hovered or selected. Gated on
  // whether *anything* is highlighted (not *which* id) so that rapidly
  // hovering across many nearby markers doesn't keep cancelling the in-flight
  // fetch before it ever resolves — the snapshot covers every entry, so it
  // stays valid across a hover session regardless of which id is current.
  const isActive = !!highlightId;
  useEffect(() => {
    if (!isActive) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await fetchTrieSnapshot();
        if (!cancelled) { setSnapshot(data); setError(false); }
      } catch {
        if (!cancelled) setError(true);
      }
    };
    poll();
    const id = setInterval(poll, POLL_INTERVAL_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, [isActive]);

  const tokenById = useMemo(() => {
    const m = new Map<string, string>();
    snapshot?.nodes.forEach(n => m.set(n.id, n.token));
    return m;
  }, [snapshot]);

  const leafToken = highlightId ? tokenById.get(highlightId) ?? null : null;

  const path = useMemo(() => {
    if (!snapshot || !leafToken) return null;
    return computePath(snapshot.nodes, leafToken);
  }, [snapshot, leafToken]);

  if (!highlightId) return null; // nothing hovered/selected — render nothing

  return (
    <div
      style={{
        background: 'rgba(15,23,42,0.92)', color: '#e2e8f0',
        borderRadius: 8, maxWidth: 380,
        fontFamily: 'monospace', fontSize: '0.72rem', lineHeight: 1.7,
        backdropFilter: 'blur(4px)', border: '1px solid rgba(255,255,255,0.07)',
        boxShadow: '0 8px 24px rgba(0,0,0,0.45)',
      }}
    >
      <div
        style={{
          display: 'flex', justifyContent: 'space-between',
          alignItems: 'center', gap: 8, padding: '8px 12px 4px',
          fontWeight: 700, fontSize: '0.78rem',
        }}
      >
        <span>S2 Trie · <span style={{ color: '#7dd3fc' }}>{highlightId}</span></span>
        {error && <span style={{ color: '#f87171', fontWeight: 400 }}>unreachable</span>}
        {!error && snapshot && (
          <span style={{ color: '#64748b', fontWeight: 400 }}>L{snapshot.s2_level}</span>
        )}
      </div>
      {!path && (
        <div style={{ color: '#f87171', padding: '0 12px 10px', maxWidth: 300 }}>
          {snapshot
            ? `"${highlightId}" isn't in the current trie cycle — likely served from Redis's TTL cache (last seen recently, but omitted from the latest poll).`
            : 'Loading trie…'}
        </div>
      )}
      {path && (
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 4, padding: '2px 12px 10px' }}>
          {path.map((step, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              {i > 0 && <span style={{ color: '#334155' }}>→</span>}
              <div className={`trie-node${i === path.length - 1 ? ' is-leaf' : ''}`}>
                <span className="trie-node-prefix">{step.prefix || 'root'}</span>
                <span className="trie-node-count">{step.count}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
