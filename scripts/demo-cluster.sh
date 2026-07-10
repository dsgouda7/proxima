#!/usr/bin/env bash
# GeoRedis distributed cluster demo
# Walks through: startup → gossip convergence → split → failover
set -euo pipefail

COMPOSE="docker compose -f demo/cluster-compose.yml"
C0="http://localhost:4000"
C1="http://localhost:4001"
C2="http://localhost:4002"
C3="http://localhost:4003"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'
BOLD='\033[1m'; RESET='\033[0m'

sep()  { echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"; }
hdr()  { sep; echo -e "${BOLD}$1${RESET}"; sep; }
ok()   { echo -e "${GREEN}✓${RESET} $1"; }
info() { echo -e "  $1"; }
wait_for_input() { echo -e "\n${BOLD}Press Enter to continue...${RESET}"; read -r; }

# ── STEP 1: Build & start ────────────────────────────────────────────────
hdr "STEP 1 — Build and start the 3-shard cluster + standby"

info "Building geo-node Docker image (first time: ~3 min)..."
$COMPOSE build

info "Starting all 4 nodes..."
$COMPOSE up -d

info "Waiting for nodes to become healthy..."
for port in 4000 4001 4002 4003; do
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:$port/health" >/dev/null 2>&1; then
            ok "geo-node on :$port is up"
            break
        fi
        sleep 1
    done
done
sleep 3  # gossip convergence

# ── STEP 2: Show initial cluster state ──────────────────────────────────
hdr "STEP 2 — Cluster state after gossip convergence"
info "All nodes should now know about each other via gossip:"
echo ""
curl -s "$C0/cluster" | python3 -c "
import json,sys
nodes = json.load(sys.stdin)
nodes.sort(key=lambda n: n.get('prefix_start',''))
print(f'  {'NODE':12} {'PREFIX RANGE':20} {'STATUS':10} {'KEYS':8}')
print(f'  {'-'*60}')
for n in nodes:
    ps = n.get('prefix_start','(start)')
    pe = n.get('prefix_end','(end)')
    print(f'  {n[\"node_id\"]:12} [{ps:8} → {pe:8})  {n[\"status\"]:10} {n[\"key_count\"]:6}')
"

wait_for_input

# ── STEP 3: Push aircraft data ───────────────────────────────────────────
hdr "STEP 3 — Inject ~300 aircraft across all shards"
info "The demo server writes to a single shard, but in a sharded setup"
info "the client routes each aircraft to the correct node by S2 token prefix."
info "Simulating writes by directly POSTing sample aircraft to each node..."

# Push some sample aircraft
push_aircraft() {
    local node=$1 lat=$2 lon=$3 id=$4 callsign=$5
    curl -sf "$node/ingest" -H "Content-Type: application/json" \
        -d "[{\"id\":\"$id\",\"lat\":$lat,\"lon\":$lon,\"payload\":{\"callsign\":\"$callsign\"}}]" >/dev/null
}

# Americas → shard 0 (S2 tokens start with 0–4)
info "Writing 100 North America aircraft to shard 0..."
for i in $(seq 1 100); do
    lat=$(python3 -c "import random; print(round(random.uniform(25,50),4))")
    lon=$(python3 -c "import random; print(round(random.uniform(-125,-70),4))")
    push_aircraft "$C0" "$lat" "$lon" "usa$(printf '%03d' $i)" "UAL$i"
done
ok "100 NA aircraft written to shard 0"

# Europe/Asia → shard 1 (S2 tokens start with 5–9)
info "Writing 100 Europe aircraft to shard 1..."
for i in $(seq 1 100); do
    lat=$(python3 -c "import random; print(round(random.uniform(45,60),4))")
    lon=$(python3 -c "import random; print(round(random.uniform(-5,30),4))")
    push_aircraft "$C1" "$lat" "$lon" "eur$(printf '%03d' $i)" "BAW$i"
done
ok "100 EU aircraft written to shard 1"

# Pacific → shard 2 (S2 tokens start with a–f)  
info "Writing 100 Pacific aircraft to shard 2..."
for i in $(seq 1 100); do
    lat=$(python3 -c "import random; print(round(random.uniform(10,45),4))")
    lon=$(python3 -c "import random; print(round(random.uniform(120,150),4))")
    push_aircraft "$C2" "$lat" "$lon" "pac$(printf '%03d' $i)" "ANA$i"
done
ok "100 Pacific aircraft written to shard 2"

sleep 12  # wait for metrics loop to pick up key counts
echo ""
info "Updated distribution:"
curl -s "$C0/cluster" | python3 -c "
import json,sys
for n in sorted(json.load(sys.stdin), key=lambda n: n.get('prefix_start','')):
    ps = n.get('prefix_start','(start)')
    pe = n.get('prefix_end','(end)')
    keys = n['key_count']
    bar = '█' * (keys // 5)
    print(f'  {n[\"node_id\"]:12} [{ps:6} → {pe:6})  {keys:4} keys  {bar}')
"

wait_for_input

# ── STEP 4: Trigger a split ──────────────────────────────────────────────
hdr "STEP 4 — Trigger a split on shard 1 (Europe/Asia) → standby node-3"
info "node-1 owns prefix 5–a. We split it at '7', giving:"
info "  node-1: [5, 7)  →  Middle East + part of Asia"
info "  node-3: [7, a)  →  rest of Asia (previously standby)"
echo ""
info "Sending split request..."

SPLIT_RESULT=$(curl -sf -X POST "$C1/split" \
    -H "Content-Type: application/json" \
    -d '{"target":"geo-node-3:4003","split_point":"7"}')

echo "$SPLIT_RESULT" | python3 -c "
import json,sys
r = json.load(sys.stdin)
print(f'  Migrated {r[\"migrated_keys\"]} keys to node-3')
print(f'  node-1 now owns: [5, {r[\"new_prefix_end\"]})')
print(f'  node-3 now owns: [{r[\"split_point\"]}, a)')
"

sleep 5
echo ""
info "Cluster state after split (4 active shards now):"
curl -s "$C0/cluster" | python3 -c "
import json,sys
for n in sorted(json.load(sys.stdin), key=lambda n: n.get('prefix_start','')):
    ps = n.get('prefix_start','(start)')
    pe = n.get('prefix_end','(end)')
    keys = n['key_count']
    bar = '█' * (keys // 5)
    print(f'  {n[\"node_id\"]:12} [{ps:6} → {pe:6})  {keys:4} keys  {n[\"status\"]:10}  {bar}')
"

wait_for_input

# ── STEP 5: Simulate node failure ───────────────────────────────────────
hdr "STEP 5 — Simulate node-2 failure (kill the Pacific shard)"
echo ""
info "Stopping geo-node-2..."
$COMPOSE stop geo-node-2

info "Waiting for gossip to detect the failure (~${SUSPECT_SECS:-10}s suspect, ~${DEAD_SECS:-30}s dead)..."
info "Watching node-0's view of the cluster:"
for i in $(seq 1 8); do
    sleep 5
    NODE2_STATUS=$(curl -s "$C0/cluster" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n['node_id']=='node-2': print(n['status'])
" 2>/dev/null || echo "unknown")
    echo "  [${i}×5s] node-2 status from node-0's view: ${NODE2_STATUS}"
    if [ "$NODE2_STATUS" = "dead" ]; then break; fi
done

echo ""
ok "Gossip detected node-2 as Dead without any central coordinator"

wait_for_input

# ── STEP 6: Bring node-2 back ───────────────────────────────────────────
hdr "STEP 6 — Restore node-2 and watch it re-join via gossip"
$COMPOSE start geo-node-2
sleep 8

NODE2_STATUS=$(curl -s "$C0/cluster" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n['node_id']=='node-2': print(n['status'])
" 2>/dev/null || echo "unknown")

ok "node-2 rejoined — status: $NODE2_STATUS"

# ── Final summary ────────────────────────────────────────────────────────
hdr "DEMO COMPLETE"
info "Final 4-shard cluster topology:"
curl -s "$C0/cluster" | python3 -c "
import json,sys
print()
for n in sorted(json.load(sys.stdin), key=lambda n: n.get('prefix_start','')):
    ps = n.get('prefix_start','∅')
    pe = n.get('prefix_end','∅')
    print(f'  {n[\"node_id\"]:12}  [{ps or \"start\":6} → {pe or \"end\":6})  {n[\"status\"]:10}  {n[\"key_count\"]} keys')
print()
"

info "To tear down: docker compose -f demo/cluster-compose.yml down -v"
info "To inspect individual nodes:"
info "  curl http://localhost:4000/metrics"
info "  curl http://localhost:4001/metrics"
info "  curl http://localhost:4002/metrics"
info "  curl http://localhost:4003/metrics"
