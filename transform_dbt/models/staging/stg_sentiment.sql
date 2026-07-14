{% if var('use_seed_data', true) %}

with raw_sentiment as (
    select * from {{ ref('raw_sentiment') }}
)

{% else %}

with raw_sentiment as (
    select * from {{ source('lakehouse', 'sentiment') }}
)

{% endif %}

select
    cast(timestamp as timestamp) as fact_timestamp,
    cast(asset_tag as varchar) as asset_ticker,
    cast(text_payload as varchar) as raw_text,
    cast(sentiment_score as double) as simulated_sentiment_score,
    cast(source as varchar) as analytics_source
from raw_sentiment
