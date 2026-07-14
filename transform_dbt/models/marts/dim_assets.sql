with unique_assets as (
    select distinct asset_ticker from {{ ref('stg_market_prices') }}
)

select
    -- md5 used here purely as a stable hash for a surrogate key, not for
    -- any cryptographic/security purpose
    md5(asset_ticker) as asset_key,
    asset_ticker,
    case 
        when asset_ticker = 'BTCUSDT' then 'Bitcoin / US Dollar Tether'
        when asset_ticker = 'ETHUSDT' then 'Ethereum / US Dollar Tether'
        when asset_ticker = 'SOLUSDT' then 'Solana / US Dollar Tether'
        else 'Unknown Crypto Asset'
    end as asset_fullname,
    'Cryptocurrency' as asset_class
from unique_assets