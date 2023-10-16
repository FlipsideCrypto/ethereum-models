{{ config(
    materialized = 'view'
) }}

SELECT
    '0xa17581a9e3356d9a858b789d68b4d866e593ae94' AS compound_market_address,
    'Compound WETH' AS compound_market_name,
    'cWETHv3' AS compound_market_symbol,
    18 AS compound_market_decimals,
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS underlying_asset_address,
    'Wrapped Ether' AS underlying_asset_name,
    'WETH' AS underlying_asset_symbol,
    18 AS underlying_asset_decimals,
    16400710 AS created_block
UNION ALL
SELECT
    '0xc3d688b66703497daa19211eedff47f25384cdc3' AS compound_market_address,
    'Compound USDC' AS compound_market_name,
    'cUSDCv3' AS compound_market_symbol,
    6 AS compound_market_decimals,
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS underlying_asset_address,
    'USDC' AS underlying_asset_name,
    'USDC' AS underlying_asset_symbol,
    6 AS underlying_asset_decimals,
    15331586 AS created_block
