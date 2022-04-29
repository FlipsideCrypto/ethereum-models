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
    _log_id,
    block_number,
    tx_hash,
    block_timestamp,
    contract_address,
    from_address,
    to_address,
    nft_tokenid AS tokenId,
    erc1155_value,
    ingested_at,
    event_index
FROM
    transfers qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
