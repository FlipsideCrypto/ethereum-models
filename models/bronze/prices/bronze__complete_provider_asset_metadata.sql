{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.bronze_complete_provider_asset_metadata(
    platforms = var('PRICES_PLATFORMS')
) }}
