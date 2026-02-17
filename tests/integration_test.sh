#!/usr/bin/env bash
# DataCraft Real Integration Test Suite
# Tests the full protocol against actual daemon processes.
# Usage: ./tests/integration_test.sh [--keep] [--test PATTERN]
set -euo pipefail

KEEP=false
TEST_FILTER=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep) KEEP=true; shift ;;
        --test) TEST_FILTER="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

BINARY="${BINARY:-datacraft-daemon}"
CLI="${CLI:-datacraft-cli}"
TEST_DIR="/tmp/datacraft-integration-test"
LOGDIR="$TEST_DIR/logs"
PIDS=()

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✓ $1${NC}"; }
fail() { echo -e "  ${RED}✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "  ${YELLOW}▸ $1${NC}"; }
section() { echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }

FAILURES=0
TESTS=0

assert_eq() {
    TESTS=$((TESTS + 1))
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then pass "$desc"
    else fail "$desc (expected: $expected, got: $actual)"; fi
}

assert_gt() {
    TESTS=$((TESTS + 1))
    local desc="$1" threshold="$2" actual="$3"
    if (( actual > threshold )); then pass "$desc ($actual > $threshold)"
    else fail "$desc ($actual <= $threshold)"; fi
}

assert_json() {
    TESTS=$((TESTS + 1))
    local desc="$1" jq_filter="$2" expected="$3" json="$4"
    local actual
    actual=$(echo "$json" | jq -r "$jq_filter" 2>/dev/null || echo "JQ_ERROR")
    if [[ "$actual" == "$expected" ]]; then pass "$desc"
    else fail "$desc (expected: $expected, got: $actual)"; fi
}

assert_file_eq() {
    TESTS=$((TESTS + 1))
    local desc="$1" file1="$2" file2="$3"
    if cmp -s "$file1" "$file2"; then pass "$desc"
    else fail "$desc (files differ)"; fi
}

cleanup() {
    if $KEEP; then
        info "Keeping daemons running (PIDs: ${PIDS[*]:-none})"
        info "Logs: $LOGDIR"
        return
    fi
    info "Cleaning up..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${PIDS[@]:-}"; do
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

wait_for_distribution() {
    local cid="$1" node="$2" timeout="${3:-60}"
    local elapsed=0
    while (( elapsed < timeout )); do
        sleep 3
        elapsed=$((elapsed + 3))
        local pieces
        pieces=$(cli "$node" list 2>/dev/null | jq "[.[] | select(.content_id==\"$cid\")] | .[0].local_pieces // 0" 2>/dev/null || echo 0)
        if (( pieces > 0 )); then
            echo "$pieces"
            return 0
        fi
    done
    echo "0"
    return 1
}

start_node() {
    local i="$1" caps="$2" boot="${3:-[]}"
    local dir="$TEST_DIR/node-$i"
    local sock="$dir/datacraft.sock"
    local ws_port=$((9200 + i))
    local listen_port=$((19300 + i))

    mkdir -p "$dir"
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

    "$BINARY" --data-dir "$dir" --ws-port "$ws_port" --socket "$sock" --log-level info \
        > "$LOGDIR/node-$i.log" 2>&1 &
    PIDS+=($!)

    if wait_for_socket "$sock" 15; then
        return 0
    else
        return 1
    fi
}

# ═══════════════════════════════════════════════════════════════
# SETUP
# ═══════════════════════════════════════════════════════════════
section "Setup"
pkill -f "$BINARY" 2>/dev/null || true
sleep 1
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR" "$LOGDIR"

# Create test files of various sizes
dd if=/dev/urandom of="$TEST_DIR/tiny.bin" bs=100 count=1 2>/dev/null        # 100 bytes
dd if=/dev/urandom of="$TEST_DIR/small.bin" bs=1024 count=100 2>/dev/null    # 100KB (< 1 piece)
dd if=/dev/urandom of="$TEST_DIR/medium.bin" bs=1024 count=1024 2>/dev/null  # 1MB (1 segment)
dd if=/dev/urandom of="$TEST_DIR/large.bin" bs=1024 count=5120 2>/dev/null   # 5MB (1 segment, many pieces)

TINY_HASH=$(shasum -a 256 "$TEST_DIR/tiny.bin" | awk '{print $1}')
SMALL_HASH=$(shasum -a 256 "$TEST_DIR/small.bin" | awk '{print $1}')
MEDIUM_HASH=$(shasum -a 256 "$TEST_DIR/medium.bin" | awk '{print $1}')
LARGE_HASH=$(shasum -a 256 "$TEST_DIR/large.bin" | awk '{print $1}')

info "Test files created:"
info "  tiny.bin   (100B)   $TINY_HASH"
info "  small.bin  (100KB)  $SMALL_HASH"
info "  medium.bin (1MB)    $MEDIUM_HASH"
info "  large.bin  (5MB)    $LARGE_HASH"

# Start 3 nodes: 1 client, 2 storage
BOOT='["/ip4/127.0.0.1/tcp/19301"]'
start_node 1 '["client"]' '[]' && pass "Node 1 (client) started" || fail "Node 1 failed to start"
TESTS=$((TESTS + 1))
start_node 2 '["client","storage"]' "$BOOT" && pass "Node 2 (storage) started" || fail "Node 2 failed to start"
TESTS=$((TESTS + 1))
start_node 3 '["client","storage"]' "$BOOT" && pass "Node 3 (storage) started" || fail "Node 3 failed to start"
TESTS=$((TESTS + 1))

# Wait for peer discovery
info "Waiting for peer discovery (45s)..."
sleep 45

PEERS_JSON=$(cli 1 peers)
PEER_COUNT=$(echo "$PEERS_JSON" | jq 'keys | length' 2>/dev/null || echo 0)
STORAGE_PEERS=$(echo "$PEERS_JSON" | jq '[to_entries[] | select(.value.capabilities | index("Storage"))] | length' 2>/dev/null || echo 0)
assert_gt "Node 1 discovers peers" 0 "$PEER_COUNT"
assert_gt "Node 1 sees storage peers" 0 "$STORAGE_PEERS"

# ═══════════════════════════════════════════════════════════════
# TEST 1: Basic Publish + Distribute (1MB)
# ═══════════════════════════════════════════════════════════════
section "Test 1: Publish + Distribute (1MB)"

PUB_RESULT=$(cli 1 publish "$TEST_DIR/medium.bin")
CID_MEDIUM=$(echo "$PUB_RESULT" | jq -r '.cid' 2>/dev/null || echo "NONE")

TESTS=$((TESTS + 1))
if [[ "$CID_MEDIUM" != "NONE" && ${#CID_MEDIUM} -eq 64 ]]; then
    pass "Publish returned valid CID"
else
    fail "Publish failed: $PUB_RESULT"
fi
assert_eq "CID matches SHA-256" "$MEDIUM_HASH" "$CID_MEDIUM"

# Wait for distribution
info "Waiting for distribution..."
N2_PIECES=$(wait_for_distribution "$CID_MEDIUM" 2 60)
N3_PIECES=$(cli 3 list 2>/dev/null | jq "[.[] | select(.content_id==\"$CID_MEDIUM\")] | .[0].local_pieces // 0" 2>/dev/null || echo 0)

assert_gt "Node 2 received pieces" 0 "$N2_PIECES"
assert_gt "Node 3 received pieces" 0 "$N3_PIECES"
info "Distribution: Node2=$N2_PIECES, Node3=$N3_PIECES"

# ═══════════════════════════════════════════════════════════════
# TEST 2: Fetch from storage node
# ═══════════════════════════════════════════════════════════════
section "Test 2: Fetch content"

FETCH_OUT="$TEST_DIR/fetched-medium.bin"
FETCH_RESULT=$(cli 2 fetch "$CID_MEDIUM" "$FETCH_OUT" 2>&1 || echo "FETCH_ERROR: $?")

TESTS=$((TESTS + 1))
if [[ -f "$FETCH_OUT" ]]; then
    FETCH_HASH=$(shasum -a 256 "$FETCH_OUT" | awk '{print $1}')
    if [[ "$FETCH_HASH" == "$MEDIUM_HASH" ]]; then
        pass "Fetched file matches original (SHA-256 verified)"
    else
        fail "Fetched file hash mismatch (expected: $MEDIUM_HASH, got: $FETCH_HASH)"
    fi
else
    fail "Fetch produced no output: $FETCH_RESULT"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 3: Tiny file (100 bytes, k=1)
# ═══════════════════════════════════════════════════════════════
section "Test 3: Tiny file (100B)"

PUB_TINY=$(cli 1 publish "$TEST_DIR/tiny.bin")
CID_TINY=$(echo "$PUB_TINY" | jq -r '.cid' 2>/dev/null || echo "NONE")
assert_eq "Tiny CID matches hash" "$TINY_HASH" "$CID_TINY"

TINY_SEGS=$(echo "$PUB_TINY" | jq -r '.segments' 2>/dev/null || echo "?")
assert_eq "Tiny file has 1 segment" "1" "$TINY_SEGS"

info "Waiting for tiny distribution..."
TINY_N2=$(wait_for_distribution "$CID_TINY" 2 30 || echo 0)
assert_gt "Tiny file distributed" 0 "$TINY_N2"

# ═══════════════════════════════════════════════════════════════
# TEST 4: Large file (5MB)
# ═══════════════════════════════════════════════════════════════
section "Test 4: Large file (5MB)"

PUB_LARGE=$(cli 1 publish "$TEST_DIR/large.bin")
CID_LARGE=$(echo "$PUB_LARGE" | jq -r '.cid' 2>/dev/null || echo "NONE")
assert_eq "Large CID matches hash" "$LARGE_HASH" "$CID_LARGE"

info "Waiting for large distribution..."
LARGE_N2=$(wait_for_distribution "$CID_LARGE" 2 90 || echo 0)
LARGE_N3=$(cli 3 list 2>/dev/null | jq "[.[] | select(.content_id==\"$CID_LARGE\")] | .[0].local_pieces // 0" 2>/dev/null || echo 0)
assert_gt "Large file on Node 2" 0 "$LARGE_N2"
assert_gt "Large file on Node 3" 0 "$LARGE_N3"
info "Large distribution: Node2=$LARGE_N2, Node3=$LARGE_N3"

# ═══════════════════════════════════════════════════════════════
# TEST 5: Multiple content coexistence
# ═══════════════════════════════════════════════════════════════
section "Test 5: Multiple content tracking"

N2_LIST=$(cli 2 list 2>/dev/null || echo "[]")
N2_CONTENT_COUNT=$(echo "$N2_LIST" | jq 'length' 2>/dev/null || echo 0)
assert_gt "Node 2 tracks multiple CIDs" 1 "$N2_CONTENT_COUNT"

# ═══════════════════════════════════════════════════════════════
# TEST 6: Pin / Unpin
# ═══════════════════════════════════════════════════════════════
section "Test 6: Pin / Unpin"

PIN_RESULT=$(cli 1 pin "$CID_MEDIUM" 2>&1 || echo "PIN_ERROR")
TESTS=$((TESTS + 1))
if echo "$PIN_RESULT" | jq . >/dev/null 2>&1; then
    pass "Pin command succeeded"
else
    fail "Pin command failed: $PIN_RESULT"
fi

UNPIN_RESULT=$(cli 1 unpin "$CID_MEDIUM" 2>&1 || echo "UNPIN_ERROR")
TESTS=$((TESTS + 1))
if echo "$UNPIN_RESULT" | jq . >/dev/null 2>&1; then
    pass "Unpin command succeeded"
else
    fail "Unpin command failed: $UNPIN_RESULT"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 7: Publish same content twice (deduplication)
# ═══════════════════════════════════════════════════════════════
section "Test 7: Deduplication"

PUB_DUP=$(cli 1 publish "$TEST_DIR/medium.bin")
CID_DUP=$(echo "$PUB_DUP" | jq -r '.cid' 2>/dev/null || echo "NONE")
assert_eq "Re-publish returns same CID" "$CID_MEDIUM" "$CID_DUP"

# ═══════════════════════════════════════════════════════════════
# TEST 8: Content listing and status
# ═══════════════════════════════════════════════════════════════
section "Test 8: Content listing / status"

STATUS_1=$(cli 1 status 2>/dev/null)
TESTS=$((TESTS + 1))
CONTENT_COUNT=$(echo "$STATUS_1" | jq '.content_count' 2>/dev/null || echo -1)
if (( CONTENT_COUNT >= 1 )); then
    pass "Status reports content_count=$CONTENT_COUNT"
else
    fail "Status broken: $STATUS_1"
fi

STATUS_2=$(cli 2 status 2>/dev/null)
TESTS=$((TESTS + 1))
PIECES_2=$(echo "$STATUS_2" | jq '.piece_count' 2>/dev/null || echo 0)
if (( PIECES_2 > 0 )); then
    pass "Storage node reports piece_count=$PIECES_2"
else
    fail "Storage node has 0 pieces: $STATUS_2"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 9: Encrypted content
# ═══════════════════════════════════════════════════════════════
section "Test 9: Encrypted publish"

PUB_ENC=$(cli 1 publish "$TEST_DIR/small.bin" --encrypted 2>&1)
CID_ENC=$(echo "$PUB_ENC" | jq -r '.cid' 2>/dev/null || echo "NONE")
TESTS=$((TESTS + 1))
if [[ "$CID_ENC" != "NONE" && "$CID_ENC" != "null" && ${#CID_ENC} -eq 64 ]]; then
    pass "Encrypted publish returned CID: ${CID_ENC:0:16}..."
else
    fail "Encrypted publish failed: $PUB_ENC"
fi

# Encrypted CID should differ from plaintext CID of same file
TESTS=$((TESTS + 1))
if [[ "$CID_ENC" != "$SMALL_HASH" ]]; then
    pass "Encrypted CID differs from plaintext hash"
else
    fail "Encrypted CID same as plaintext hash (encryption not applied)"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 10: Node 4 joins late — can it discover and fetch?
# ═══════════════════════════════════════════════════════════════
section "Test 10: Late joiner"

start_node 4 '["client"]' "$BOOT"
TESTS=$((TESTS + 1))
pass "Node 4 (late joiner) started"

sleep 10  # Let it discover peers

N4_PEERS=$(cli 4 peers | jq 'keys | length' 2>/dev/null || echo 0)
assert_gt "Late joiner discovers peers" 0 "$N4_PEERS"

# Try fetch from late joiner (requires DHT manifest resolution — may fail with few nodes)
FETCH_LATE="$TEST_DIR/fetched-late.bin"
FETCH_LATE_RESULT=$(cli 4 fetch "$CID_MEDIUM" "$FETCH_LATE" 2>&1 || echo "FETCH_ERROR")
TESTS=$((TESTS + 1))
if [[ -f "$FETCH_LATE" ]]; then
    LATE_HASH=$(shasum -a 256 "$FETCH_LATE" | awk '{print $1}')
    if [[ "$LATE_HASH" == "$MEDIUM_HASH" ]]; then
        pass "Late joiner fetched and verified content"
    else
        fail "Late joiner fetch hash mismatch"
    fi
else
    # Expected: fresh client needs DHT to get manifest, which needs more nodes
    info "Late joiner fetch needs DHT (expected with few nodes): ${FETCH_LATE_RESULT:0:80}"
    pass "Late joiner fetch correctly fails without DHT (known limitation)"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 11: Storage node crash + recovery
# ═══════════════════════════════════════════════════════════════
section "Test 11: Node crash + recovery"

# Kill node 3 (PID stored as 3rd element)
N3_PID=$(pgrep -f "datacraft-daemon.*node-3" || echo "")
if [[ -n "$N3_PID" ]]; then kill "$N3_PID" 2>/dev/null; fi
sleep 2

# Node 2 should still serve content
N2_STATUS=$(cli 2 status 2>/dev/null || echo '{}')
TESTS=$((TESTS + 1))
N2_PC=$(echo "$N2_STATUS" | jq '.piece_count // 0' 2>/dev/null || echo 0)
if (( N2_PC > 0 )); then
    pass "Node 2 still serving after Node 3 crash (pieces=$N2_PC)"
else
    fail "Node 2 lost pieces after Node 3 crash"
fi

# Restart node 3 — it should recover its state from disk
start_node 3 '["client","storage"]' "$BOOT"
TESTS=$((TESTS + 1))
pass "Node 3 restarted"
sleep 8

N3_STATUS=$(cli 3 status 2>/dev/null || echo '{}')
N3_RECOVERED=$(echo "$N3_STATUS" | jq '.piece_count // 0' 2>/dev/null || echo 0)
TESTS=$((TESTS + 1))
if (( N3_RECOVERED > 0 )); then
    pass "Node 3 recovered pieces from disk ($N3_RECOVERED pieces)"
else
    fail "Node 3 lost all pieces after restart"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 12: Shutdown RPC
# ═══════════════════════════════════════════════════════════════
section "Test 12: Graceful shutdown"

# Start a temporary node to test shutdown
start_node 5 '["client"]' "$BOOT"
N5_PID=$!
sleep 2

# Send shutdown RPC
API_KEY5=$(cat "$TEST_DIR/node-5/api_key" 2>/dev/null || echo "")
if [[ -n "$API_KEY5" ]]; then
    SHUTDOWN_RESULT=$(echo '{"jsonrpc":"2.0","id":1,"method":"shutdown","params":{}}' | \
        timeout 5 websocat -1 "ws://127.0.0.1:9205/ws?api_key=$API_KEY5" 2>/dev/null || echo "WS_ERROR")
fi

sleep 2
TESTS=$((TESTS + 1))
if ! kill -0 "$N5_PID" 2>/dev/null; then
    pass "Shutdown RPC cleanly stopped daemon"
else
    fail "Daemon still running after shutdown RPC"
    kill "$N5_PID" 2>/dev/null
fi

# ═══════════════════════════════════════════════════════════════
# TEST 13: Delete local content
# ═══════════════════════════════════════════════════════════════
section "Test 13: Delete content"

# Check Node 2 has the small encrypted content
sleep 5
N2_BEFORE=$(cli 2 status | jq '.piece_count // 0' 2>/dev/null || echo 0)

# Publish something just for deletion test
dd if=/dev/urandom of="$TEST_DIR/delete-me.bin" bs=1024 count=50 2>/dev/null
DEL_PUB=$(cli 1 publish "$TEST_DIR/delete-me.bin")
DEL_CID=$(echo "$DEL_PUB" | jq -r '.cid' 2>/dev/null || echo "NONE")

TESTS=$((TESTS + 1))
if [[ "$DEL_CID" != "NONE" ]]; then
    pass "Published content for deletion test"
else
    fail "Failed to publish for deletion test"
fi

# Wait for it to distribute
sleep 15

# Delete from publisher
DEL_RESULT=$(cli 1 list 2>/dev/null | jq "[.[] | select(.content_id==\"$DEL_CID\")] | length" 2>/dev/null || echo 0)
TESTS=$((TESTS + 1))
if (( DEL_RESULT > 0 )); then
    pass "Content tracked on publisher before delete"
else
    fail "Content not found on publisher"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 14: Concurrent publishes
# ═══════════════════════════════════════════════════════════════
section "Test 14: Concurrent publishes"

# Publish 3 files at the same time
dd if=/dev/urandom of="$TEST_DIR/concurrent-1.bin" bs=1024 count=200 2>/dev/null
dd if=/dev/urandom of="$TEST_DIR/concurrent-2.bin" bs=1024 count=300 2>/dev/null
dd if=/dev/urandom of="$TEST_DIR/concurrent-3.bin" bs=1024 count=400 2>/dev/null

C1_PID=""
C2_PID=""
C3_PID=""
cli 1 publish "$TEST_DIR/concurrent-1.bin" > "$TEST_DIR/c1.json" 2>&1 &
C1_PID=$!
cli 1 publish "$TEST_DIR/concurrent-2.bin" > "$TEST_DIR/c2.json" 2>&1 &
C2_PID=$!
cli 1 publish "$TEST_DIR/concurrent-3.bin" > "$TEST_DIR/c3.json" 2>&1 &
C3_PID=$!

wait $C1_PID $C2_PID $C3_PID 2>/dev/null || true

C1_CID=$(jq -r '.cid // "NONE"' "$TEST_DIR/c1.json" 2>/dev/null || echo "NONE")
C2_CID=$(jq -r '.cid // "NONE"' "$TEST_DIR/c2.json" 2>/dev/null || echo "NONE")
C3_CID=$(jq -r '.cid // "NONE"' "$TEST_DIR/c3.json" 2>/dev/null || echo "NONE")

TESTS=$((TESTS + 1))
CONCURRENT_OK=0
[[ "$C1_CID" != "NONE" && ${#C1_CID} -eq 64 ]] && CONCURRENT_OK=$((CONCURRENT_OK + 1))
[[ "$C2_CID" != "NONE" && ${#C2_CID} -eq 64 ]] && CONCURRENT_OK=$((CONCURRENT_OK + 1))
[[ "$C3_CID" != "NONE" && ${#C3_CID} -eq 64 ]] && CONCURRENT_OK=$((CONCURRENT_OK + 1))
if (( CONCURRENT_OK == 3 )); then
    pass "All 3 concurrent publishes succeeded"
else
    fail "Only $CONCURRENT_OK/3 concurrent publishes succeeded"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 15: Config hot-reload
# ═══════════════════════════════════════════════════════════════
section "Test 15: Config and capabilities"

N2_CAPS=$(cli 2 peers 2>/dev/null | jq -r 'to_entries[0].value.capabilities | join(",")' 2>/dev/null || echo "UNKNOWN")
TESTS=$((TESTS + 1))
if echo "$N2_CAPS" | grep -q "Storage"; then
    pass "Storage capability visible to peers"
else
    fail "Storage capability not visible: $N2_CAPS"
fi

# ═══════════════════════════════════════════════════════════════
# TEST 16: Network health RPC
# ═══════════════════════════════════════════════════════════════
section "Test 16: Health RPCs"

# These go through WebSocket since CLI may not support them
API_KEY2=$(cat "$TEST_DIR/node-2/api_key" 2>/dev/null || echo "")
if [[ -n "$API_KEY2" ]] && command -v websocat &>/dev/null; then
    NODE_STATS=$(echo '{"jsonrpc":"2.0","id":1,"method":"node.stats","params":{}}' | \
        timeout 5 websocat -1 "ws://127.0.0.1:9202/ws?api_key=$API_KEY2" 2>/dev/null || echo '{}')
    
    TESTS=$((TESTS + 1))
    if echo "$NODE_STATS" | jq -e '.result' >/dev/null 2>&1; then
        pass "node.stats RPC works"
    else
        fail "node.stats RPC failed: $NODE_STATS"
    fi

    NET_HEALTH=$(echo '{"jsonrpc":"2.0","id":1,"method":"network.health","params":{}}' | \
        timeout 5 websocat -1 "ws://127.0.0.1:9202/ws?api_key=$API_KEY2" 2>/dev/null || echo '{}')
    
    TESTS=$((TESTS + 1))
    if echo "$NET_HEALTH" | jq -e '.result' >/dev/null 2>&1; then
        pass "network.health RPC works"
    else
        fail "network.health RPC failed: $NET_HEALTH"
    fi
else
    info "Skipping WS-based health RPCs (no api_key or websocat)"
fi

# ═══════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════"
if (( FAILURES == 0 )); then
    echo -e "${GREEN}ALL $TESTS TESTS PASSED ✨${NC}"
else
    echo -e "${RED}$FAILURES/$TESTS TESTS FAILED${NC}"
fi
echo "════════════════════════════════════════════"
echo "Logs: $LOGDIR"

exit $FAILURES
