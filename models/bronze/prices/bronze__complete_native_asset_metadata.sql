{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.bronze_complete_native_asset_metadata(
    symbols = var('PRICES_SYMBOLS')
) }}
