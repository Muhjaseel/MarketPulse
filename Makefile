# =====================================================================
# MarketPulse — Makefile
# =====================================================================

.PHONY: up down restart ps logs-all dbt-run dbt-debug dbt-test streamlit test lint clean-data

# --- Docker infrastructure ---
up:
	docker compose -f docker/docker-compose.yaml up -d

down:
	docker compose -f docker/docker-compose.yaml down

restart: down up

ps:
	docker compose -f docker/docker-compose.yaml ps

logs-all:
	docker compose -f docker/docker-compose.yaml logs -f

# --- dbt ---
dbt-debug:
	cd transform_dbt && dbt debug

dbt-run:
	cd transform_dbt && dbt seed && dbt run

dbt-test:
	cd transform_dbt && dbt test

# --- Dashboard ---
streamlit:
	streamlit run dashboards/streamlit/app.py

# --- Python tests & lint ---
test:
	pytest tests/ -v

lint:
	ruff check src/ dashboards/ dags/ tests/

# --- Cleanup ---
clean-data: down
	docker volume rm $$(docker volume ls -q | grep marketpulse) || true
	rm -rf transform_dbt/marketpulse_local.db transform_dbt/target/ transform_dbt/logs/
