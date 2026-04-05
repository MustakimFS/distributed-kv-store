#!/bin/bash

echo "========================================="
echo " Distributed KV Store - Benchmark Suite"
echo "========================================="

cd "$(dirname "$0")/.." || exit 1

echo ""
echo "Running unit and integration tests..."
mvn test

echo ""
echo "========================================="
echo " Fetching latency report from node1..."
echo "========================================="
docker compose logs node1 | grep -A 20 "Latency Report"

echo ""
echo "========================================="
echo " Fault Tolerance Test"
echo "========================================="
echo "Killing node1 (current or potential leader)..."
docker compose stop node1

echo "Waiting 2 seconds for re-election..."
sleep 2

echo "Checking for new leader election..."
docker compose logs | grep -i "became LEADER" | tail -5

echo ""
echo "Restarting node1..."
docker compose start node1
sleep 2

echo ""
echo "========================================="
echo " All tests complete."
echo "========================================="