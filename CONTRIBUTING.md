# Contributing to MarketPulse

This is a personal portfolio project, but it's set up like a real one so
it's easy to pick up.

## Local setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

See the README for the full local setup (Docker services, dbt, running the
dashboard).

## Before opening a PR

```bash
ruff check src/ dashboards/ dags/ tests/
pytest tests/ -v
cd transform_dbt && dbt seed && dbt run && dbt test
```

All three must pass - this is exactly what `.github/workflows/ci.yml` runs.

## Guidelines

- No new technology gets added to the README unless there's real, wired-up
  code exercising it (this project explicitly avoids listing
  aspirational-but-unused tech - see README > Known Limitations for what's
  intentionally left out and why).
- Keep dbt models tested: any new model in `transform_dbt/models/` should
  get a corresponding entry in a `schema.yml` (`not_null` / `unique` /
  `accepted_values` at minimum).
- Keep secrets out of the repo. Copy `.env.example` to `.env` for local
  overrides; `.env` is gitignored.
