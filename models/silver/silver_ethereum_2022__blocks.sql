{{ config(
    materialized = 'incremental',
    unique_key = 'log_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
    tags = ['snowflake', 'ethereum', 'silver_ethereum', 'ethereum_transfers']
) }}

WITH logs AS (

    SELECT
        log_id,
        block_id,
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
        log_id,
        block_id,
        tx_hash,
        block_timestamp,
        contract_address :: STRING AS contract_address,
        event_inputs :from :: STRING AS from_address,
        event_inputs :to :: STRING AS to_address,
        event_inputs :value :: FLOAT AS raw_amount,
        ingested_at
    FROM
        logs
    WHERE
        event_name = 'Transfer'
        AND raw_amount IS NOT NULL
)
SELECT
    *
FROM
    transfers qualify(ROW_NUMBER() over(PARTITION BY log_id
ORDER BY
    ingested_at DESC)) = 1
