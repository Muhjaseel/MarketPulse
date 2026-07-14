with prices as (
    select * from {{ ref('stg_market_prices') }}
),

sentiment as (
    -- One row per (asset_ticker, minute) rather than the raw per-post
    -- grain: the price feed ticks once per minute per ticker, but a real
    -- sentiment stream can easily produce several posts inside the same
    -- minute. Collapsing to the most recent post per bucket up front
    -- keeps the join below one-to-one and prevents fanning out
    -- (duplicating) price rows when multiple sentiment rows share a
    -- minute bucket.
    select
        asset_ticker,
        date_trunc('minute', fact_timestamp) as sentiment_minute,
        rolling_sentiment_score
    from {{ ref('int_sentiment_scores') }}
    qualify row_number() over (
        partition by asset_ticker, date_trunc('minute', fact_timestamp)
        order by fact_timestamp desc
    ) = 1
),

price_moves as (
    select
        fact_timestamp,
        asset_ticker,
        asset_price,
        lag(asset_price) over (
            partition by asset_ticker
            order by fact_timestamp
        ) as prior_price
    from prices
),

joined as (
    select
        p.fact_timestamp,
        p.asset_ticker,
        p.asset_price,
        coalesce(s.rolling_sentiment_score, 0.0) as social_sentiment_score,
        case
            when p.prior_price is null or p.prior_price = 0 then 0.0
            else greatest(0.0, (p.prior_price - p.asset_price) / p.prior_price)
        end as price_drop_ratio,
        case
            when coalesce(s.rolling_sentiment_score, 0.0) < 0
            then abs(coalesce(s.rolling_sentiment_score, 0.0))
            else 0.0
        end as negative_sentiment_intensity
    from price_moves as p
    -- Bucketed to the minute rather than an exact timestamp match: the two
    -- producers are independent processes stamping their own wall-clock
    -- time, so in live streaming (not seed data, which happens to align
    -- to the minute) an exact-equality join would rarely find a match.
    -- Minute-bucketing keeps the join meaningful once real streams are
    -- running instead of only working by coincidence against the seeds.
    left join sentiment as s
        on p.asset_ticker = s.asset_ticker
        and date_trunc('minute', p.fact_timestamp) = s.sentiment_minute
)

select
    fact_timestamp,
    asset_ticker,
    asset_price,
    least(1.0, (price_drop_ratio * 0.6) + (negative_sentiment_intensity * 0.4)) as market_panic_index,
    social_sentiment_score
from joined
