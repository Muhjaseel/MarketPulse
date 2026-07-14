{% if var('use_seed_data', true) %}

with raw_prices as (
    select * from {{ ref('raw_market_prices') }}
)

{% else %}

with raw_prices as (
    select * from {{ source('lakehouse', 'prices') }}
)

{% endif %}

select
    cast(timestamp as timestamp) as fact_timestamp,
    cast(ticker as varchar) as asset_ticker,
    cast(price as double) as asset_price,
    cast(volume as double) as traded_volume
from raw_prices
