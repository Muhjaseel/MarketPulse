-- A dbt test fails if this query returns any rows.
-- Checks two invariants of fact_market_panic_index in one pass:
--   1. market_panic_index must stay within its documented [0.0, 1.0] range.
--   2. (asset_ticker, fact_timestamp) must be unique - the grain the
--      dashboard and downstream marts assume.

with out_of_bounds as (
    select
        asset_ticker,
        fact_timestamp,
        market_panic_index,
        'out_of_bounds' as failure_reason
    from {{ ref('fact_market_panic_index') }}
    where market_panic_index < 0.0 or market_panic_index > 1.0
),

duplicate_grain as (
    select
        asset_ticker,
        fact_timestamp,
        null as market_panic_index,
        'duplicate_grain' as failure_reason
    from {{ ref('fact_market_panic_index') }}
    group by asset_ticker, fact_timestamp
    having count(*) > 1
)

select * from out_of_bounds
union all
select * from duplicate_grain
