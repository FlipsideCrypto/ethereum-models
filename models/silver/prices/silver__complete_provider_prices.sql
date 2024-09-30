{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_provider_prices_id',
    cluster_by = ['recorded_hour::DATE','provider'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id),SUBSTRING(asset_id)",
    tags = ['non_realtime']
) }}
{{ fsc_evm.silver_complete_provider_prices() }}
