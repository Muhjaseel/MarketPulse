with sentiment_stream as (
    select * from {{ ref('stg_sentiment') }}
),

scored as (
    select
        fact_timestamp,
        asset_ticker,
        raw_text,
        simulated_sentiment_score,
        analytics_source,
        avg(simulated_sentiment_score) over (
            partition by asset_ticker
            order by fact_timestamp
            rows between 4 preceding and current row
        ) as rolling_sentiment_score
    from sentiment_stream
)

select * from scored
