{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}
{{ fsc_evm.price_dim_asset_metadata() }}