#!/usr/bin/env bash

echo "Stopping MarketPulse..."
echo "=========================================================="

echo "Terminating local streaming processes (if running)..."
pkill -f "src.producers.social_sentiment_stream" 2>/dev/null
pkill -f "src.producers.market_price_stream" 2>/dev/null
pkill -f "src.consumers.duckdb_sink" 2>/dev/null
pkill -f "streamlit run" 2>/dev/null

echo "Stopping Docker services..."
docker compose -f docker/docker-compose.yaml down

echo "=========================================================="
echo "Shutdown complete."
echo "=========================================================="