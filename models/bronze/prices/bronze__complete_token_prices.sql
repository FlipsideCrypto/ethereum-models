{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.bronze_complete_token_prices() }}
