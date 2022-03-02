{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = ['ingested_at::DATE']
) }}

WITH base_tables AS (

    SELECT
        record_id,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        ingested_at
    FROM
        {{ ref('bronze__blocks') }}

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
    block_id :: INTEGER AS block_number,
    block_timestamp :: TIMESTAMP AS block_timestamp,
    network :: STRING AS network,
    chain_id :: STRING AS blockchain,
    tx_count :: INTEGER AS tx_count,
    header :difficulty AS difficulty,
    COALESCE(
        header: total_difficulty,
        header :totalDifficulty
    ) AS total_difficulty,
    header: extra_data :: STRING AS extra_data,
    -- might be able to parse this for more details
    COALESCE(
        header :gas_limit,
        header :gasLimit
    ) AS gas_limit,
    COALESCE(
        header :gas_used,
        header :gasUsed
    ) AS gas_used,
    header: "hash" :: STRING AS HASH,
    COALESCE(
        header: parent_hash :: STRING,
        header: parentHash :: STRING
    ) AS parent_hash,
    header: miner :: STRING AS miner,
    header: nonce :: STRING AS nonce,
    COALESCE(
        header: receipts_root :: STRING,
        header: receiptsRoot :: STRING
    ) AS receipts_root,
    header: sha3_uncles :: STRING AS sha3_uncles,
    header: "size" AS SIZE,
    CASE
        WHEN header: uncles [1] :: STRING IS NOT NULL THEN CONCAT(
            header: uncles [0] :: STRING,
            ', ',
            header: uncles [1] :: STRING
        )
        ELSE header: uncles [0] :: STRING
    END AS uncle_blocks,
    ingested_at :: TIMESTAMP AS ingested_at
FROM
    base_tables qualify(ROW_NUMBER() over(PARTITION BY block_number
ORDER BY
    ingested_at DESC)) = 1 -- removing integer cast on difficulty, total difficulty, and size 2/22/22 due to funky blocks: 9561058, 9555083, 9555105, 9561008, 9561063, 9561116, 9555052
