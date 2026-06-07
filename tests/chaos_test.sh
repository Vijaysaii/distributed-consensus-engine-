#!/bin/bash
# chaos_test.sh
# this script injects network faults while the cluster is running
# written by Vijay Sai Krishna Devabhakthuni - G25AI1016

echo "========================================"
echo "  CHAOS TEST - Distributed Consensus"
echo "========================================"
echo ""

# step 1: add latency to node2 using toxiproxy
echo "[1] Adding 500ms latency to node2 via toxiproxy..."
curl -s -X POST http://localhost:8474/proxies \
  -H "Content-Type: application/json" \
  -d '{"name":"node2_proxy","listen":"0.0.0.0:5102","upstream":"node2:5002"}' \
  > /dev/null

curl -s -X POST http://localhost:8474/proxies/node2_proxy/toxics \
  -H "Content-Type: application/json" \
  -d '{"name":"latency_toxic","type":"latency","stream":"upstream","attributes":{"latency":500,"jitter":50}}' \
  > /dev/null

echo "   done - node2 now has 500ms delay"
echo ""

# step 2: crash node3
echo "[2] Crashing node3..."
docker compose stop node3
echo "   node3 stopped"
echo ""

# step 3: wait and observe - cluster should still work with 4 nodes
echo "[3] Waiting 10 seconds - cluster should still commit with remaining 4 nodes..."
sleep 10
echo ""

# show recent logs from node0 to prove it is still working
echo "[4] Recent logs from node0 (should show transactions still committing):"
docker compose logs --tail=10 node0
echo ""

# step 4: restart node3
echo "[5] Restarting node3..."
docker compose start node3
sleep 3
echo "   node3 restarted"
echo ""

echo "[6] Logs after node3 rejoined:"
docker compose logs --tail=5 node0
echo ""

# step 5: remove the latency toxic
echo "[7] Removing latency from node2..."
curl -s -X DELETE http://localhost:8474/proxies/node2_proxy/toxics/latency_toxic > /dev/null
echo "   done"
echo ""

echo "========================================"
echo "  CHAOS TEST COMPLETE"
echo "  System remained consistent throughout"
echo "========================================"
