{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    cluster_by = ['_inserted_timestamp::DATE']
) }}
