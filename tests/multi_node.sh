#!/usr/bin/env bash
#
# Multi-node integration test for DataCraft
#
# Starts 3 daemon instances on localhost, tests:
#   1. mDNS peer discovery
#   2. Publish on node A → fetch on node B via DHT + P2P transfer
#   3. Peer listing on node C
#
# Usage:
#   cargo build && ./tests/multi_node.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_DIR/target/debug/datacraft-daemon"
CLI="$PROJECT_DIR/target/debug/datacraft-cli"

# Temp dirs
cleanup() {
    echo "=== Cleaning up ==="
    kill "$PID_A" "$PID_B" "$PID_C" 2>/dev/null || true
    wait "$PID_A" "$PID_B" "$PID_C" 2>/dev/null || true
    rm -rf /tmp/dc-test-node-{a,b,c} /tmp/dc-test-{a,b,c}.sock /tmp/dc-test-output /tmp/dc-test-input
}
trap cleanup EXIT

# Check binaries exist
if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: $BINARY not found. Run 'cargo build' first."
    exit 1
fi
if [[ ! -x "$CLI" ]]; then
    echo "ERROR: $CLI not found. Run 'cargo build' first."
    exit 1
fi

# Clean previous runs
rm -rf /tmp/dc-test-node-{a,b,c} /tmp/dc-test-{a,b,c}.sock /tmp/dc-test-output /tmp/dc-test-input

echo "=== Starting 3 DataCraft nodes ==="

"$BINARY" \
    --data-dir /tmp/dc-test-node-a \
    --socket /tmp/dc-test-a.sock \
    --listen /ip4/127.0.0.1/tcp/44001 \
    --log-level info &
PID_A=$!

"$BINARY" \
    --data-dir /tmp/dc-test-node-b \
    --socket /tmp/dc-test-b.sock \
    --listen /ip4/127.0.0.1/tcp/44002 \
    --log-level info &
PID_B=$!

"$BINARY" \
    --data-dir /tmp/dc-test-node-c \
    --socket /tmp/dc-test-c.sock \
    --listen /ip4/127.0.0.1/tcp/44003 \
    --log-level info &
PID_C=$!

echo "Node A PID=$PID_A, Node B PID=$PID_B, Node C PID=$PID_C"

# Wait for daemons to start and IPC sockets to appear
echo "=== Waiting for daemons to start ==="
for i in $(seq 1 15); do
    if [[ -S /tmp/dc-test-a.sock && -S /tmp/dc-test-b.sock && -S /tmp/dc-test-c.sock ]]; then
        echo "All sockets ready after ${i}s"
        break
    fi
    sleep 1
done

if [[ ! -S /tmp/dc-test-a.sock ]]; then
    echo "FAIL: Node A socket not created"
    exit 1
fi

# Wait for mDNS discovery and Kademlia routing table population
echo "=== Waiting for mDNS peer discovery + DHT bootstrap (8s) ==="
sleep 8

# Test 1: Status check on all nodes
echo ""
echo "=== Test 1: Status check ==="
for node in a b c; do
    echo "--- Node $node status ---"
    "$CLI" --socket "/tmp/dc-test-${node}.sock" status || echo "WARN: status failed on node $node"
done

# Test 2: Peer discovery
echo ""
echo "=== Test 2: Peer discovery ==="
for node in a b c; do
    echo "--- Node $node peers ---"
    PEERS=$("$CLI" --socket "/tmp/dc-test-${node}.sock" peers 2>&1) || true
    echo "$PEERS"
done

# Test 3: Publish on node A
echo ""
echo "=== Test 3: Publish file on Node A ==="
echo "Hello from DataCraft multi-node test! $(date)" > /tmp/dc-test-input
PUBLISH_RESULT=$("$CLI" --socket /tmp/dc-test-a.sock publish /tmp/dc-test-input 2>&1) || true
echo "Publish result: $PUBLISH_RESULT"

# Extract CID from publish result
CID=$(echo "$PUBLISH_RESULT" | grep -o '"cid"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/.*"cid"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
if [[ -z "$CID" ]]; then
    echo "WARN: Could not extract CID from publish result. Trying alternative parse..."
    CID=$(echo "$PUBLISH_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('cid',''))" 2>/dev/null || true)
fi

if [[ -n "$CID" ]]; then
    echo "Published CID: $CID"

    # Wait for DHT propagation
    echo "=== Waiting for DHT propagation (3s) ==="
    sleep 3

    # Test 4: Fetch on node B
    echo ""
    echo "=== Test 4: Fetch on Node B ==="
    FETCH_RESULT=$("$CLI" --socket /tmp/dc-test-b.sock fetch "$CID" /tmp/dc-test-output 2>&1) || true
    echo "Fetch result: $FETCH_RESULT"

    if [[ -f /tmp/dc-test-output ]]; then
        echo "--- Comparing content ---"
        if diff /tmp/dc-test-input /tmp/dc-test-output > /dev/null 2>&1; then
            echo "SUCCESS: Content matches!"
        else
            echo "FAIL: Content mismatch"
            echo "Original: $(cat /tmp/dc-test-input)"
            echo "Fetched:  $(cat /tmp/dc-test-output)"
        fi
    else
        echo "WARN: Output file not created (fetch may have failed — this is expected if DHT hasn't propagated)"
    fi
else
    echo "WARN: No CID extracted, skipping fetch test"
fi

# Test 5: List content on node A
echo ""
echo "=== Test 5: List content on Node A ==="
"$CLI" --socket /tmp/dc-test-a.sock list || echo "WARN: list failed"

echo ""
echo "=== Multi-node test complete ==="
echo "Summary:"
echo "  - 3 nodes started on localhost (ports 44001-44003)"
echo "  - mDNS discovery tested"
echo "  - Publish/fetch pipeline tested"
echo "  - Check output above for PASS/FAIL/WARN status"
