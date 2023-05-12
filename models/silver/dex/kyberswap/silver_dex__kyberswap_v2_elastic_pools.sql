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
        TRY_TO_NUMBER(ethereum.public.udf_hex_to_int(topics [3] :: STRING)) AS swapFeeUnits,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS tickDistance,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address = '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a' --Elastic Pool Deployer
        AND topics [0] :: STRING = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' --Create pool

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
    swapFeeUnits AS swap_fee_units,
    tickDistance AS tick_distance,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation
