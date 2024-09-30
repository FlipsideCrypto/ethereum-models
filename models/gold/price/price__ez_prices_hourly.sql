{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}
{{ fsc_evm.price_ez_prices_hourly() }}