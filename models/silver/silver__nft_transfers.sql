{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH logs AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_inputs,
        ingested_at :: TIMESTAMP AS ingested_at
    FROM
        {{ ref('silver__logs') }}

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
),
transfers AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address :: STRING AS contract_address,
        event_inputs :from :: STRING AS from_address,
        event_inputs :to :: STRING AS to_address,
        event_inputs :tokenId :: FLOAT AS tokenId,
        ingested_at
    FROM
        logs
    WHERE
        event_name = 'Transfer'
        AND tokenId IS NOT NULL
)
SELECT
    _log_id,
    block_number,
    tx_hash,
    block_timestamp,
    contract_address,
    from_address,
    to_address,
    tokenId,
    ingested_at
FROM
    transfers qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
