{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}
