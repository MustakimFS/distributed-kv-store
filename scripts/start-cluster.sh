#!/bin/bash

echo "========================================="
echo " Distributed KV Store - Starting Cluster"
echo "========================================="

# Build the project first
echo ""
echo "[1/3] Building project..."
cd "$(dirname "$0")/.." || exit 1
./mvnw clean package -DskipTests -q 2>/dev/null || mvn clean package -DskipTests -q

if [ $? -ne 0 ]; then
    echo "ERROR: Build failed. Run 'mvn clean package' to see errors."
    exit 1
fi
echo "Build successful."

# Start the cluster
echo ""
echo "[2/3] Starting 5-node cluster with Docker Compose..."
docker compose up -d --build

if [ $? -ne 0 ]; then
    echo "ERROR: Docker Compose failed. Is Docker running?"
    exit 1
fi

# Wait for nodes to start and elect a leader
echo ""
echo "[3/3] Waiting for cluster to initialize (10 seconds)..."
sleep 10

# Show running containers
echo ""
echo "========================================="
echo " Cluster Status"
echo "========================================="
docker compose ps

echo ""
echo "Checking logs for leader election..."
echo "-----------------------------------------"
docker compose logs | grep -i "became LEADER" | tail -5

echo ""
echo "========================================="
echo " Cluster is ready!"
echo " Nodes running on ports 50051 - 50055"
echo "========================================="