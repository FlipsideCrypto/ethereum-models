{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.bronze_complete_provider_prices() }}
