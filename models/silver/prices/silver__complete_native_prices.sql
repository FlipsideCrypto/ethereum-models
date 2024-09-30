{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_native_prices_id',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, symbol, name),SUBSTRING(asset_id, symbol, name)",
    tags = ['non_realtime']
) }}
{{ fsc_evm.silver_complete_native_prices() }}
