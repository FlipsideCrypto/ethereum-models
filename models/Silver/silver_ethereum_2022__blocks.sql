{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
    tags = ['snowflake', 'ethereum', 'silver_ethereum', 'ethereum_blocks']
) }}

WITH base_tables AS (

    SELECT
        *
    FROM
        {{ ref('bronze_ethereum_2022__blocks') }}

{% if is_incremental() %}
WHERE
    ingested_at >= (
        SELECT
            MAX(
                ingested_at
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_id :: INTEGER AS block_id,
    block_timestamp :: TIMESTAMP AS block_timestamp,
    network :: STRING AS network,
    chain_id :: STRING AS blockchain,
    tx_count :: INTEGER AS tx_count,
    -- header :blockHeight :: INTEGER AS block_height, -- i dont think we need this - block_id maps to block_height
    -- header :blockTime :: INTEGER AS block_time,  -- i dont think we need this - block_timestamp maps to blockTime
    header :difficulty :: INTEGER AS difficulty,
    header: total_difficulty :: INTEGER AS total_difficulty,
    header: extra_data :: STRING AS extra_data,
    header :gas_limit :: INTEGER AS gas_limit,
    header :gas_used :: INTEGER AS gas_used,
    header: HASH :: STRING AS HASH,
    header: parent_hash :: STRING AS parent_hash,
    header: miner :: STRING AS miner,
    header: nonce :: STRING AS nonce,
    header: receipts_root :: STRING AS receipts_root,
    header: sha3_uncles :: STRING AS sha3_uncles,
    header: SIZE :: INTEGER AS SIZE,
    header: uncles AS uncles -- this one is still in json format, there can be more than one in a block, might need for testing but idk how useful this is to community
    ingested_at :: TIMESTAMP AS ingested_at
FROM
    base_tables qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
    ingested_at DESC)) = 1
