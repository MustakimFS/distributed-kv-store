#!/bin/bash

echo "Stopping cluster..."
cd "$(dirname "$0")/.." || exit 1
docker compose down

echo "Cluster stopped."