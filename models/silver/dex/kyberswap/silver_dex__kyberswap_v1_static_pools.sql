{{ config(
    materialized = 'incremental',
    unique_key = "pool_address"
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS ampBps,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS feeUnits,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS totalPool,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address = LOWER('0x1c758aF0688502e49140230F6b0EBd376d429be5') --static pool factory
        AND topics [0] :: STRING = '0xb6bce363b712c921bead4bcc977289440eb6172eb89e258e3a25bd49ca806de6' --create pool

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    token0,
    token1,
    pool_address,
    ampBps AS amp_bps,
    feeUnits AS fee_units,
    totalPool AS total_pool,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation
