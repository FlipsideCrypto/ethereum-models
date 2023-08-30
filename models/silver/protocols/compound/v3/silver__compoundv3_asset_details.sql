{{ config(
    materialized = 'view'
) }}


select 
    '0xa17581a9e3356d9a858b789d68b4d866e593ae94' as compound_market_address,
    'Compound WETH' as compound_market_name,
    'cWETHv3' as compound_market_symbol,
    18 as compound_market_decimals,
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as underlying_asset_address
union all
select 
    '0xc3d688b66703497daa19211eedff47f25384cdc3' as compound_market_address,
    'Compound USDC' as compound_market_name,
    'cUSDCv3' as compound_market_symbol,
    6 as compound_market_decimals,
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as underlying_asset_address