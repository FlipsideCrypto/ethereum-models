{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.bronze_complete_native_prices(
    symbols = var('PRICES_SYMBOLS')
) }}
