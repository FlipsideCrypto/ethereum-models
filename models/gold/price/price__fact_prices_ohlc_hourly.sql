{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}
{{ fsc_evm.price_fact_prices_ohlc_hourly() }}
