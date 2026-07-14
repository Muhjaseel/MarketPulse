#!/usr/bin/env bash

echo "🛑 Halting and tearing down all background MarketPulse Docker services..."
# Safely stop and remove all cluster containers, networks, and anonymous storage volumes
docker compose -f docker/docker-compose.yaml down -v

echo "🧹 Purging cached local storage caches and binary database artifacts..."

# 1. Wipe out local compiled DuckDB databases
if [ -f "transform_dbt/marketpulse_local.db" ]; then
    rm transform_dbt/marketpulse_local.db
    echo "🗑️ Removed target: transform_dbt/marketpulse_local.db"
fi

# 2. Wipe out compiled dbt artifacts (target logs and cached model files)
if [ -d "transform_dbt/target" ]; then
    rm -rf transform_dbt/target
    rm -rf transform_dbt/dbt_packages
    echo "🗑️ Cleared out dbt target compilation artifacts."
fi

# 3. Clean up any loose python compilation caches
find . -type d -name "__pycache__" -exec rm -rf {} + &>/dev/null
find . -type f -name "*.pyc" -delete &>/dev/null

echo "✨ System cleanup process finalized! Your workspace sandbox is perfectly reset."