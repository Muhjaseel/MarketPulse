# MarketPulse

A local streaming data platform that ingests simulated crypto market prices
and social sentiment, lands them through Redpanda into DuckDB, transforms
them with dbt, gates bad data with Great Expectations, orchestrates the
whole thing with Airflow, and serves the result in a Streamlit dashboard.

Built as a portfolio project to demonstrate real data engineering
fundamentals — not as a production trading system, and not with any
component pretending to be more than it is.

## Motivation

Most portfolio "data pipeline" projects either (a) never actually run, or
(b) claim a stack of trendy tools that aren't really wired together. This
project is an attempt to do the opposite: every technology named below is
genuinely in the data path, every test genuinely runs and passes, and the
README's "Known Limitations" section says out loud what isn't real yet
(the sentiment scores, for one) instead of hiding it.

## Architecture

```
market_price_stream.py          social_sentiment_stream.py
(random walk simulator)          (template + jitter simulator)
        |                                |
        +---------------+----------------+
                         v
                  Redpanda (Kafka API)
                         |
                         v
           src/consumers/duckdb_sink.py
     (consumes both topics, writes raw rows into
        raw_market_prices / raw_sentiment)
                         |
                         v
             DuckDB (marketpulse_local.db)
                         |
        +----------------+-----------------+
        v                                  v
  dbt staging models              Great Expectations gate
  (stg_market_prices,              (assert_data_contracts,
   stg_sentiment)                   an Airflow task — bad batches
        |                           route to a quarantine branch
        v                           instead of silently flowing on)
  dbt intermediate + marts models
  (int_sentiment_scores, int_volatility, dim_assets,
   fact_market_panic_index, fact_market_anomalies)
        |
        v
  Streamlit dashboard
  (reads the DuckDB file read-only, auto-refreshes)
```

Airflow (`dags/marketpulse_master_pipeline.py`) runs on a `*/5 * * * *`
schedule: build staging views -> run the data-quality gate -> branch to
either compile the marts or quarantine the batch -> mark the run complete.

### A note on the price/sentiment join

`fact_market_panic_index.sql` joins the price and sentiment streams on a
minute-bucketed timestamp (`date_trunc('minute', ...)`), not an exact
match, and dedupes sentiment to one row per `(ticker, minute)` before
joining. That's deliberate: the two producers are independent processes
stamping their own wall-clock time, so an exact-timestamp join only
"works" against the seed CSVs (which happen to align to the minute) and
would rarely find a match against two live-running streams. Bucketing —
plus deduping sentiment first so a busy minute with several posts doesn't
fan out and duplicate price rows — is what actually makes the join hold up
once both simulators are running for real.

## Technology stack

| Layer | Technology | What it actually does here |
|---|---|---|
| Streaming | Redpanda (Kafka API) | Two topics (`market_prices`, `social_sentiment`); simulators produce, `duckdb_sink.py` consumes |
| Storage / warehouse | DuckDB | Single local file (`transform_dbt/marketpulse_local.db`); landing tables + all dbt models |
| Transformation | dbt (dbt-duckdb adapter) | Staging -> intermediate -> marts, with real schema tests + a singular SQL test |
| Data quality | Great Expectations (Core/Fluent API) | Row-level checks on the staging batches inside the Airflow DAG; failing batches branch to quarantine instead of proceeding |
| Orchestration | Apache Airflow (LocalExecutor) | The 5-task DAG described above, plus OpenLineage instrumentation |
| Metadata DB | Postgres | Airflow's own metadata store |
| Dashboard | Streamlit | Reads the DuckDB file read-only; 4 pages (overview, panic index, sentiment, volatility) |
| CI | GitHub Actions | Lint (ruff) + `pytest tests/` (20 tests, including a real `dbt seed && dbt run && dbt test`) + Docker build validation |

## Folder structure

```
MarketPulse/
├── dags/                     # Airflow DAG
├── src/
│   ├── producers/            # market_price_stream.py, social_sentiment_stream.py (simulators)
│   ├── consumers/            # duckdb_sink.py
│   ├── quality/               # gx_validation.py (Great Expectations gate)
│   └── utils/                 # config_loader, duckdb_client, logger, constants
├── transform_dbt/            # dbt project (staging / intermediate / marts + schema.yml tests)
├── dashboards/streamlit/     # app.py + pages/
├── docker/                   # docker-compose.yaml + Dockerfiles for airflow/dashboard
├── tests/                    # pytest suite (unit tests + dbt integration test)
├── scripts/                  # start_project.sh, stop_project.sh, bootstrap-kafka.sh, cleanup.sh
└── .github/workflows/ci.yml  # real CI: lint, test, docker build
```

## Local setup

Prerequisites: Docker, Python 3.11+.

```bash
git clone <this-repo>
cd MarketPulse
cp .env.example .env      # then edit credentials if you're not just running this locally
pip install -r requirements.txt

# Start Redpanda, Postgres, Airflow, and the dashboard
make up

# Build the dbt models against the seed data (works with no streams running)
make dbt-run

# In separate terminals, start the streaming simulators + sink to see
# fresh data flow in instead of just the seeds:
python -m src.producers.market_price_stream
python -m src.producers.social_sentiment_stream
python -m src.consumers.duckdb_sink

# Dashboard
make streamlit
```

Or just run `./scripts/start_project.sh`, which does the compose-up, dbt
build, and dashboard launch in sequence and prints the commands for the
simulators.

Airflow UI: http://localhost:8080 (default admin/admin — see `.env.example`
for why that's fine locally and not anywhere else). Redpanda Console:
http://localhost:8888. Dashboard: http://localhost:8501.

## Example output

Once `make dbt-run` has built the marts against the seed data, the
dashboard's landing page shows a per-ticker scorecard (spot price + panic
score) pulled straight from `fact_market_panic_index`; the Panic Index,
Sentiment, and Volatility pages each drill into one mart. There's no
screenshot checked into this repo — a static image of a dashboard that's
designed to auto-refresh every 2 seconds off a locally-generated DuckDB
file would go stale immediately and isn't worth the honesty tradeoff of a
picture nobody can verify against the running code. Run
`./scripts/start_project.sh` and open http://localhost:8501 to see it live
in under a minute.

## Troubleshooting

- **Dashboard shows "Analytical Database not compiled yet."** You haven't
  run `make dbt-run` (or `cd transform_dbt && dbt seed && dbt run`) yet —
  the dashboard reads `transform_dbt/marketpulse_local.db`, which only
  exists after dbt builds it.
- **`KeyError` / connection errors from the producers on startup.**
  Redpanda usually needs a few seconds after `docker compose up` before
  it's actually ready to accept connections. `scripts/start_project.sh`
  sleeps 10s for this; if you're running `make up` manually and starting a
  producer immediately after, give it a moment or check
  `docker compose -f docker/docker-compose.yaml logs redpanda`.
- **`dbt` command not found.** `dbt-core`/`dbt-duckdb` are in
  `requirements.txt`; make sure you've activated the virtualenv you ran
  `pip install -r requirements.txt` in before calling `make dbt-run` or
  `dbt` directly.
- **Airflow task fails with a DuckDB "conflicting lock" error.** Expected
  under contention — `src/utils/duckdb_client.py` retries writes on lock
  errors, and the dashboard's `query_dataframe` degrades to an empty
  result rather than crashing. If it persists, check whether a stray
  `dbt run` or Streamlit session is holding the file open elsewhere
  (`lsof transform_dbt/marketpulse_local.db` on macOS/Linux).
- **Local pandas behaves differently than in Docker.** Shouldn't happen —
  `pandas==2.2.2` is now pinned identically across `requirements.txt`,
  `docker/airflow-requirements.txt`, and
  `dashboards/streamlit/requirements.txt`. If you've bumped one, bump all
  three together (see the comment at the top of `requirements.txt`).

## Interview discussion topics

Things worth being able to speak to confidently if this project comes up
in a screening, roughly in the order an interviewer is likely to probe:

- **Why minute-bucket the panic-index join instead of an exact
  timestamp match?** (see "A note on the price/sentiment join" above) —
  and why the sentiment side is deduped to one row per bucket *before*
  the join, not after.
- **Why `trigger_rule="none_failed_min_one_success"` on
  `pipeline_complete`** in the DAG, instead of the Airflow default
  (`all_success`) — without it, the join task after a `BranchPythonOperator`
  gets permanently skipped whenever the branch takes the other path.
- **What delivery guarantee does `duckdb_sink.py` actually give you?**
  `enable_auto_commit=True` with `auto_offset_reset="latest"` is roughly
  at-least-once with real gaps (a crash between consuming and committing
  can still lose a message) — worth being able to say plainly rather than
  overclaiming exactly-once semantics that aren't implemented.
- **Why is `simulated_sentiment_score` named that way and not
  `nlp_sentiment_score`?** Because it isn't NLP-derived — it's a jittered
  hardcoded score from `social_sentiment_stream.py`'s template posts, and
  the column name should say so instead of implying more than what's
  there.
- **What would break first at real production scale?** The single-file
  DuckDB warehouse — no concurrent-writer story beyond the lock-retry
  logic in `duckdb_client.py`, and no horizontal scale path without
  swapping to a real warehouse (see Known Limitations below).

## Testing

```bash
pytest tests/ -v
```

20 tests, all real and all currently passing:
- Unit tests for the DuckDB connection helpers (`test_duckdb_client.py`)
- Unit tests for the config loader's fallback behavior (`test_config_loader.py`)
- Unit tests for the Great Expectations data-quality gate, including
  engineered failure cases (`test_gx_validation.py`)
- An integration test that runs the real dbt project (`dbt seed && dbt run
  && dbt test`) against the seed data and checks the panic-index math and
  bounds directly from the compiled DuckDB tables
  (`test_panic_index_integration.py`)

Plus 24 dbt tests (`cd transform_dbt && dbt test`): not-null/unique/
accepted-value checks on every model, and a singular test asserting the
panic index stays in `[0.0, 1.0]` and is unique per (ticker, timestamp).

## Recent fixes

In the same spirit as removing the unwired Iceberg/MinIO claims below: a
technical audit of this repo found a handful of real, verifiable issues,
and rather than leave them for someone else to find, they're fixed and
documented here.

- **Dead config reference.** `market_price_stream.py` read
  `app_settings.yaml["market"]["mock_stream_rate_per_second"]`, but
  `app_settings.yaml` never defined a `market:` block — the lookup always
  raised `KeyError`, silently caught by a bare `except Exception`, so the
  configured rate never actually applied. Added the missing block.
- **Inconsistent pandas pins.** `requirements.txt` pinned `pandas==3.0.2`
  while the Airflow and dashboard requirements files pinned `2.2.2` —
  meaning the local test suite and the Docker containers weren't running
  the same stack. All three now pin `2.2.2`.
- **Panic-index join only worked against seed data.** See "A note on the
  price/sentiment join" above.
- **Duplicate volatility logic.** `volatility_dashboard.py` recomputed
  rolling mean/stddev in pandas that `int_volatility.sql` /
  `fact_market_anomalies.sql` already compute in dbt. The page now queries
  `fact_market_anomalies` directly instead of maintaining a second copy of
  the same math.
- **`ROWID`-based "latest row" queries.** `app.py` and
  `panic_dashboard.py` both picked each ticker's "latest" row by ordering
  on DuckDB's `ROWID`. Since `fact_market_panic_index` is a
  `+materialized: table` that dbt fully rebuilds every run, `ROWID` has no
  guaranteed relationship to chronological order. Both now order by
  `fact_timestamp` instead.
- **`app.py` duplicated DB path logic.** Every page under
  `dashboards/streamlit/pages/` already imported `resolve_db_path` /
  `db_exists` / `query_dataframe` from the shared `db.py`; `app.py` alone
  re-resolved the DuckDB path inline. It now imports from `db.py` like
  everything else.
- **Misleading column name.** `nlp_sentiment_score` implied a trained NLP
  model that doesn't exist in this project. Renamed to
  `simulated_sentiment_score` everywhere (staging model, intermediate
  model, schema tests, the GX validator, the sentiment dashboard page, and
  the test suite) to match what it actually is.
- **GX gate instantiated at import time.** `dq_gate` used to run
  `gx.get_context()` as a side effect of importing `gx_validation.py`.
  It's now built lazily on first use instead.

## Known limitations

Being direct about what's not real yet, since that's the whole point of
this rewrite:

- **Sentiment is simulated, not NLP.** `social_sentiment_stream.py` picks
  from 8 hand-written template posts and jitters a hardcoded score. There
  is no trained sentiment model in this project. A real version would
  swap that producer for one that scores actual scraped/streamed text
  with something like VADER or a fine-tuned transformer.
- **No object-store lakehouse layer.** An earlier version of this project
  referenced Apache Iceberg and MinIO, but neither was ever wired to a
  real read/write path — the actual warehouse is a single local DuckDB
  file. Those references have been removed rather than left as
  aspirational claims. A real next step would be landing raw data in
  MinIO/S3 as Iceberg tables and pointing dbt at that instead of the
  local file.
- **Single-file DuckDB, no true horizontal scale.** Fine for a portfolio
  project's data volumes; would need a real warehouse (Snowflake,
  BigQuery, a distributed Postgres, etc.) at production scale.
- **No auth/network hardening beyond local Docker defaults.** The
  Airflow admin/admin default and open ports are fine for `docker compose
  up` on localhost and are not meant to be exposed to a network.
- **At-least-once-ish consumer, not exactly-once.**
  `duckdb_sink.py`'s Kafka consumer uses `enable_auto_commit=True` with
  `auto_offset_reset="latest"`, which is fine for a portfolio simulator
  but doesn't give hard delivery guarantees — a crash between consuming a
  message and the periodic auto-commit can still lose it. A production
  version would commit offsets only after a confirmed DuckDB write.

## Future improvements

- Real sentiment scoring (VADER or a small fine-tuned transformer) in
  place of the template simulator.
- A real object-store landing zone (MinIO + Iceberg or real S3) feeding
  dbt, instead of the local DuckDB file.
- Great Expectations checks on the marts layer, not just staging.
- A second, independent portfolio project (a batch ETL pipeline) to show
  range beyond streaming.

## License

MIT — see [LICENSE](LICENSE).
