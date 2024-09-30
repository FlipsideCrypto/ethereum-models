{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_provider_asset_metadata_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)",
    tags = ['non_realtime']
) }}
{{ fsc_evm.silver_complete_provider_asset_metadata() }}