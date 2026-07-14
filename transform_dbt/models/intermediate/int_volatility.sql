with price_stream as (
    select * from {{ ref('stg_market_prices') }}
),

enriched as (
    select
        fact_timestamp,
        asset_ticker,
        asset_price,
        traded_volume,
        avg(asset_price) over (
            partition by asset_ticker
            order by fact_timestamp
            rows between 4 preceding and current row
        ) as rolling_avg_price,
        stddev_pop(asset_price) over (
            partition by asset_ticker
            order by fact_timestamp
            rows between 4 preceding and current row
        ) as rolling_price_volatility
    from price_stream
)

select * from enriched
