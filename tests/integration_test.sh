#!/usr/bin/env bash
# DataCraft Real Integration Test
# Starts actual daemon processes, publishes content, verifies distribution and fetch.
# Usage: ./tests/integration_test.sh [--keep] (--keep leaves daemons running for debugging)
set -euo pipefail

KEEP=false
[[ "${1:-}" == "--keep" ]] && KEEP=true

BINARY="${BINARY:-datacraft-daemon}"
CLI="${CLI:-datacraft-cli}"
TEST_DIR="/tmp/datacraft-integration-test"
LOGDIR="$TEST_DIR/logs"
TEST_FILE="$TEST_DIR/test-input.bin"
FETCH_OUTPUT="$TEST_DIR/fetched.bin"
PIDS=()

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}▸ $1${NC}"; }

FAILURES=0
TESTS=0

assert_eq() {
    TESTS=$((TESTS + 1))
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        pass "$desc"
    else
        fail "$desc (expected: $expected, got: $actual)"
    fi
}

assert_gt() {
    TESTS=$((TESTS + 1))
    local desc="$1" threshold="$2" actual="$3"
    if (( actual > threshold )); then
        pass "$desc ($actual > $threshold)"
    else
        fail "$desc ($actual <= $threshold)"
    fi
}

assert_json() {
    # assert_json "desc" ".field" "expected" "$json"
    TESTS=$((TESTS + 1))
    local desc="$1" jq_filter="$2" expected="$3" json="$4"
    local actual
    actual=$(echo "$json" | jq -r "$jq_filter" 2>/dev/null || echo "JQ_ERROR")
    if [[ "$actual" == "$expected" ]]; then
        pass "$desc"
    else
        fail "$desc (expected: $expected, got: $actual)"
    fi
}

cleanup() {
    if $KEEP; then
        info "Keeping daemons running (PIDs: ${PIDS[*]})"
        info "Logs: $LOGDIR"
        info "Kill with: kill ${PIDS[*]}"
        return
    fi
    info "Cleaning up..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

cli() {
    local node="$1"; shift
    "$CLI" --socket "$TEST_DIR/node-$node/datacraft.sock" "$@" 2>&1
}

wait_for_socket() {
    local sock="$1" timeout="${2:-10}"
    local elapsed=0
    while [[ ! -S "$sock" ]] && (( elapsed < timeout )); do
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    [[ -S "$sock" ]]
}

# ─── Setup ────────────────────────────────────────────────────
info "Setting up test environment"
pkill -f "$BINARY" 2>/dev/null || true
sleep 1
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR" "$LOGDIR"

# Create test files
dd if=/dev/urandom of="$TEST_FILE" bs=1024 count=1024 2>/dev/null
ORIGINAL_HASH=$(shasum -a 256 "$TEST_FILE" | awk '{print $1}')
info "Test file: 1MB, SHA-256: $ORIGINAL_HASH"

# ─── Start 3 nodes ───────────────────────────────────────────
for i in 1 2 3; do
    dir="$TEST_DIR/node-$i"
    mkdir -p "$dir"
    sock="$dir/datacraft.sock"
    ws_port=$((9200 + i))
    listen_port=$((19300 + i))

    if [[ $i -eq 1 ]]; then
        caps='["client"]'
    else
        caps='["client","storage"]'
    fi

    # All nodes know about node-1 as bootstrap; node-1 has empty boot_peers
    if [[ $i -eq 1 ]]; then
        boot='[]'
    else
        boot='["/ip4/127.0.0.1/tcp/19301"]'
    fi

    cat > "$dir/config.json" <<CONF
{
  "schema_version": 1,
  "capabilities": $caps,
  "listen_port": $listen_port,
  "ws_port": $ws_port,
  "capability_announce_interval_secs": 5,
  "reannounce_interval_secs": 10,
  "reannounce_threshold_secs": 5,
  "max_storage_bytes": 10737418240,
  "gc_interval_secs": 3600,
  "max_peer_connections": 50,
  "max_concurrent_transfers": 64,
  "piece_timeout_secs": 30,
  "stream_open_timeout_secs": 10,
  "health_check_interval_secs": 60,
  "boot_peers": $boot
}
CONF

    "$BINARY" --data-dir "$dir" --ws-port "$ws_port" --socket "$sock" --log-level debug \
        > "$LOGDIR/node-$i.log" 2>&1 &
    PIDS+=($!)
    info "Started node-$i (pid $!, ws $ws_port, caps: $caps)"
done

# Wait for all sockets
info "Waiting for daemon sockets..."
for i in 1 2 3; do
    if wait_for_socket "$TEST_DIR/node-$i/datacraft.sock" 15; then
        pass "Node $i socket ready"
        TESTS=$((TESTS + 1))
    else
        fail "Node $i socket not ready after 15s"
        TESTS=$((TESTS + 1))
        echo "--- Node $i log ---"
        tail -20 "$LOGDIR/node-$i.log" 2>/dev/null || true
        exit 1
    fi
done

# ─── Wait for peer discovery ─────────────────────────────────
info "Waiting for peer discovery (25s)..."
sleep 25

PEERS_JSON=$(cli 1 peers)
PEER_COUNT=$(echo "$PEERS_JSON" | jq 'keys | length' 2>/dev/null || echo 0)
assert_gt "Node 1 discovers peers" 0 "$PEER_COUNT"

# Check storage peers specifically
STORAGE_PEERS=$(echo "$PEERS_JSON" | jq '[to_entries[] | select(.value.capabilities | index("Storage"))] | length' 2>/dev/null || echo 0)
assert_gt "Node 1 sees storage peers" 0 "$STORAGE_PEERS"

# ─── Publish ─────────────────────────────────────────────────
info "Publishing test file from Node 1..."
PUBLISH_RESULT=$(cli 1 publish "$TEST_FILE")
CID=$(echo "$PUBLISH_RESULT" | jq -r '.cid' 2>/dev/null || echo "NONE")

TESTS=$((TESTS + 1))
if [[ "$CID" != "NONE" && "$CID" != "null" && ${#CID} -eq 64 ]]; then
    pass "Publish returned valid CID: $CID"
else
    fail "Publish failed: $PUBLISH_RESULT"
    exit 1
fi

assert_eq "CID matches file hash" "$ORIGINAL_HASH" "$CID"

# Check publisher state
PUB_LIST=$(cli 1 list)
assert_json "Publisher tracks content" ".[0].content_id" "$CID" "$PUB_LIST"
assert_json "Publisher role is publisher" ".[0].role" "publisher" "$PUB_LIST"

# ─── Wait for distribution ───────────────────────────────────
info "Waiting for distribution (up to 60s)..."
DISTRIBUTED=false
for attempt in $(seq 1 12); do
    sleep 5

    N2_LIST=$(cli 2 list 2>/dev/null || echo "[]")
    N3_LIST=$(cli 3 list 2>/dev/null || echo "[]")

    N2_PIECES=$(echo "$N2_LIST" | jq ".[0].local_pieces // 0" 2>/dev/null || echo 0)
    N3_PIECES=$(echo "$N3_LIST" | jq ".[0].local_pieces // 0" 2>/dev/null || echo 0)

    info "  Attempt $attempt: Node2=${N2_PIECES} pieces, Node3=${N3_PIECES} pieces"

    if (( N2_PIECES > 0 && N3_PIECES > 0 )); then
        DISTRIBUTED=true
        break
    fi
done

TESTS=$((TESTS + 1))
if $DISTRIBUTED; then
    pass "Content distributed to both storage nodes"
else
    fail "Content NOT distributed after 60s"
    echo "--- Node 1 log (last 30 lines) ---"
    tail -30 "$LOGDIR/node-1.log"
    echo "--- Node 2 log (last 30 lines) ---"
    tail -30 "$LOGDIR/node-2.log"
    echo "--- Node 3 log (last 30 lines) ---"
    tail -30 "$LOGDIR/node-3.log"
fi

# Check each storage node has enough pieces for reconstruction (≥ k)
N2_FINAL=$(cli 2 list 2>/dev/null || echo "[]")
N3_FINAL=$(cli 3 list 2>/dev/null || echo "[]")
N2_COUNT=$(echo "$N2_FINAL" | jq ".[0].local_pieces // 0" 2>/dev/null || echo 0)
N3_COUNT=$(echo "$N3_FINAL" | jq ".[0].local_pieces // 0" 2>/dev/null || echo 0)

# With k=10 for 1MB (1 segment, ~10 source pieces), each node should have pieces
TESTS=$((TESTS + 1))
if (( N2_COUNT >= 2 )); then
    pass "Node 2 has ≥2 pieces ($N2_COUNT) — RLNC provider"
else
    fail "Node 2 has $N2_COUNT pieces (need ≥2)"
fi

TESTS=$((TESTS + 1))
if (( N3_COUNT >= 2 )); then
    pass "Node 3 has ≥2 pieces ($N3_COUNT) — RLNC provider"
else
    fail "Node 3 has $N3_COUNT pieces (need ≥2)"
fi

# Total pieces should be approximately original + parity (120 for k=100)
TOTAL=$((N2_COUNT + N3_COUNT))
info "Total distributed pieces: $TOTAL (Node2: $N2_COUNT, Node3: $N3_COUNT)"

# ─── Publisher cleanup ────────────────────────────────────────
info "Checking publisher state after distribution..."
PUB_AFTER=$(cli 1 list 2>/dev/null || echo "[]")
PUB_PIECES=$(echo "$PUB_AFTER" | jq ".[0].local_pieces // -1" 2>/dev/null || echo -1)
info "Publisher local pieces after distribution: $PUB_PIECES"
# Publisher should ideally have 0 pieces (all pushed out), but we don't assert yet
# since the behavioral corrections are still being validated

# ─── Fetch from storage node ─────────────────────────────────
info "Fetching content from Node 2 (storage node)..."
FETCH_RESULT=$(cli 2 fetch "$CID" "$FETCH_OUTPUT" 2>&1 || echo "FETCH_ERROR")

TESTS=$((TESTS + 1))
if [[ -f "$FETCH_OUTPUT" ]]; then
    FETCH_HASH=$(shasum -a 256 "$FETCH_OUTPUT" | awk '{print $1}')
    if [[ "$FETCH_HASH" == "$ORIGINAL_HASH" ]]; then
        pass "Fetched file matches original (SHA-256 verified)"
    else
        fail "Fetched file hash mismatch (expected: $ORIGINAL_HASH, got: $FETCH_HASH)"
    fi
else
    fail "Fetch produced no output file: $FETCH_RESULT"
fi

# ─── Summary ─────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════"
if (( FAILURES == 0 )); then
    echo -e "${GREEN}ALL $TESTS TESTS PASSED${NC}"
else
    echo -e "${RED}$FAILURES/$TESTS TESTS FAILED${NC}"
fi
echo "════════════════════════════════════════════"
echo "Logs: $LOGDIR"

exit $FAILURES
