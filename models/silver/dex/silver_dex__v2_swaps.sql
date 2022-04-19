{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH uni_v2_pairs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f'
        AND event_name = 'PairCreated'
)
