#!/usr/bin/env bash
set -e

echo "Starting MarketPulse..."
echo "=========================================================="

# 1. Start the backend infrastructure (Redpanda, Postgres, Airflow)
echo "Step 1: docker compose up -d"
docker compose -f docker/docker-compose.yaml up -d

echo "Waiting 10s for services to become healthy..."
sleep 10

# 2. Create Kafka/Redpanda topics if the bootstrap script is present
if [ -f "scripts/bootstrap-kafka.sh" ]; then
    echo "Step 2: bootstrapping topics via scripts/bootstrap-kafka.sh"
    bash scripts/bootstrap-kafka.sh
else
    echo "Step 2: skipping topic bootstrap (script not found); relying on auto-creation."
fi

# 3. Seed and build the dbt models so the dashboard has data on first load
echo "Step 3: dbt seed && dbt run"
cd transform_dbt && dbt seed && dbt run && cd ..

echo "=========================================================="
echo "Infrastructure is up."
echo "=========================================================="
echo "To start the streaming simulators, run these in separate terminals:"
echo "   -> python3 -m src.producers.social_sentiment_stream"
echo "   -> python3 -m src.producers.market_price_stream"
echo "   -> python3 -m src.consumers.duckdb_sink"
echo "=========================================================="

# 4. Launch the dashboard
echo "Step 4: streamlit run dashboards/streamlit/app.py"
streamlit run dashboards/streamlit/app.py