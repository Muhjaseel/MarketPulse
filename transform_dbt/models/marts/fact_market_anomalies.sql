with volatility_stream as (
    select * from {{ ref('int_volatility') }}
)

select
    md5(asset_ticker || cast(fact_timestamp as varchar)) as anomaly_key,
    fact_timestamp,
    asset_ticker,
    asset_price,
    traded_volume,
    coalesce(rolling_price_volatility, 0.0) as local_volatility_score,
    -- Flag an anomaly if current volatility spikes significantly above normal thresholds
    case 
        when rolling_price_volatility > (rolling_avg_price * 0.02) and traded_volume > 2.0 then true
        else false
    end as is_structural_anomaly
from volatility_stream