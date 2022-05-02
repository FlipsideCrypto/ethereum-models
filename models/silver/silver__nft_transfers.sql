{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH transfers AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        event_index,
        contract_address :: STRING AS contract_address,
        COALESCE(
            event_inputs :from :: STRING,
            event_inputs :_from :: STRING
        ) AS from_address,
        COALESCE(
            event_inputs :to :: STRING,
            event_inputs :_to :: STRING
        ) AS to_address,
        COALESCE(
            event_inputs :tokenId :: STRING,
            event_inputs :_id :: STRING,
            event_inputs :_tokenId :: STRING
        ) AS nft_tokenid,
        event_inputs :_value :: STRING AS erc1155_value,
        ingested_at
    FROM
        {{ ref('silver__logs') }}
    WHERE
        event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND nft_tokenid IS NOT NULL

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
),
labels AS (
    SELECT
        address AS project_address,
        label,
        1 AS rnk
    FROM
        {{ ref('core__dim_labels') }}
    WHERE
        address IN (
            SELECT
                DISTINCT contract_address
            FROM
                transfers
        )
),
backup_meta AS (
    SELECT
        address AS project_address,
        NAME AS label,
        2 AS rnk
    FROM
        {{ source(
            'ethereum_silver',
            'token_meta_backup'
        ) }}
    WHERE
        address IN (
            SELECT
                DISTINCT contract_address
            FROM
                transfers
        )
),
meta_union AS (
    SELECT
        project_address,
        label,
        rnk
    FROM
        labels
    UNION ALL
    SELECT
        project_address,
        label,
        rnk
    FROM
        backup_meta
),
unique_meta AS (
    SELECT
        project_address,
        label,
        rnk
    FROM
        meta_union qualify(ROW_NUMBER() over(PARTITION BY project_address
    ORDER BY
        rnk ASC)) = 1
)
SELECT
    _log_id,
    block_number,
    tx_hash,
    block_timestamp,
    CASE
        WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
        ELSE 'other'
    END AS event_type,
    contract_address,
    label AS project_name,
    from_address,
    to_address,
    nft_tokenid AS tokenId,
    erc1155_value,
    ingested_at,
    event_index
FROM
    transfers
    LEFT JOIN unique_meta
    ON unique_meta.project_address = transfers.contract_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
