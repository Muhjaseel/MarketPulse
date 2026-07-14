#!/usr/bin/env bash
# =====================================================================
# Idempotent bootstrap for the single-container Airflow service.
# Runs DB migrations + admin-user creation exactly once per volume,
# then starts the scheduler in the background and the webserver in the
# foreground so `docker compose up airflow` yields one healthy service.
# =====================================================================
set -euo pipefail

echo "🗄️  Applying Airflow metadata database migrations..."
airflow db migrate

ADMIN_USERNAME="${_AIRFLOW_ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${_AIRFLOW_ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${_AIRFLOW_ADMIN_EMAIL:-admin@marketpulse.local}"

if ! airflow users list | grep -q "^${ADMIN_USERNAME} "; then
  echo "👤 Creating default Airflow admin user '${ADMIN_USERNAME}'..."
  airflow users create \
    --username "${ADMIN_USERNAME}" \
    --password "${ADMIN_PASSWORD}" \
    --firstname MarketPulse \
    --lastname Admin \
    --role Admin \
    --email "${ADMIN_EMAIL}"
else
  echo "👤 Admin user '${ADMIN_USERNAME}' already exists, skipping creation."
fi

echo "⏱️  Starting Airflow scheduler in the background..."
airflow scheduler &
SCHEDULER_PID=$!

trap 'echo "🛑 Shutting down..."; kill -TERM "${SCHEDULER_PID}" 2>/dev/null || true; wait' SIGTERM SIGINT

echo "🌐 Starting Airflow webserver in the foreground..."
airflow webserver &
WEBSERVER_PID=$!

wait -n "${SCHEDULER_PID}" "${WEBSERVER_PID}"
EXIT_CODE=$?
echo "❌ One of the Airflow processes exited (code ${EXIT_CODE}); shutting down the container."
kill -TERM "${SCHEDULER_PID}" "${WEBSERVER_PID}" 2>/dev/null || true
exit "${EXIT_CODE}"
