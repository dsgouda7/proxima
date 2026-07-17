// Geo-node HTTP base URLs the cluster monitor polls, in priority order.
// The first reachable node is used for /cluster reads and as the origin
// for split/merge control-plane calls.
//
// Default targets the local Docker Compose cluster (demo/cluster-compose.yml).
//
// To point this UI at a Kubernetes-deployed cluster instead:
//   1. kubectl apply -k demo/k8s/
//   2. Port-forward each shard's Service to a local port, e.g.:
//        kubectl port-forward -n geo-redis svc/geo-node-0 4000:4000
//        kubectl port-forward -n geo-redis svc/geo-node-1 4001:4001
//        kubectl port-forward -n geo-redis svc/geo-node-2 4002:4002
//        kubectl port-forward -n geo-redis svc/geo-node-3 4003:4003
//   3. Run the UI with the same local ports (no override needed), or point
//      it at different ports/hosts via VITE_CLUSTER_NODES, e.g.:
//        VITE_CLUSTER_NODES="http://localhost:4000,http://localhost:4001,http://localhost:4002,http://localhost:4003" npm run dev
const raw = import.meta.env.VITE_CLUSTER_NODES as string | undefined;

export const CLUSTER_NODES: string[] = raw
  ? raw.split(',').map(s => s.trim()).filter(Boolean)
  : [
      'http://localhost:4000',
      'http://localhost:4001',
      'http://localhost:4002',
      'http://localhost:4003',
    ];

/// Address the source shard should use to reach the standby node when
/// triggering a split. In Docker Compose this is the internal service DNS
/// name; in Kubernetes it would be the standby Service's cluster-internal
/// address (e.g. "geo-node-3.geo-redis.svc.cluster.local:4003"), since the
/// splitting node calls this address from inside the cluster, not from
/// the browser. Override via VITE_STANDBY_TARGET if not using Compose.
export const STANDBY_TARGET: string =
  (import.meta.env.VITE_STANDBY_TARGET as string | undefined) || 'geo-node-3:4003';
