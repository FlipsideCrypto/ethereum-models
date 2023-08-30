{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH base_events AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_status,
        contract_address,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x253553366da8546fc250f225fe3d25d0c782303b',
            --ETHRegistrarController
            '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e' --ENSRegistryWithFallback
        )
        AND topics [0] :: STRING IN (
            '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27',
            --NameRegistered
            '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0' --NewResolver
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
name_registered AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics [1] :: STRING AS label,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS owner,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS baseCost,
        baseCost / pow(
            10,
            18
        ) AS base_cost_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS premium,
        premium / pow(
            10,
            18
        ) AS premium_adj,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS expires,
        TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
        utils.udf_hex_to_string(
            segmented_data [5] :: STRING
        ) AS NAME,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topics [0] :: STRING = '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27'
),
new_resolver AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics [1] :: STRING AS node,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS resolver,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topics [0] :: STRING = '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0' qualify (ROW_NUMBER() over (PARTITION BY _log_id
    ORDER BY
        block_timestamp DESC)) = 1
)
SELECT
    r.block_number,
    r.block_timestamp,
    r.tx_hash,
    r.event_index,
    r.origin_from_address,
    r.origin_to_address,
    r.origin_function_signature,
    r.contract_address,
    label,
    owner,
    baseCost AS base_cost_raw,
    base_cost_adj,
    premium AS premium_raw,
    premium_adj,
    expires,
    expires_timestamp,
    NAME,
    node,
    resolver,
    r._log_id AS _log_id,
    r._inserted_timestamp
FROM
    name_registered r
    LEFT JOIN new_resolver n
    ON r.block_number = n.block_number
    AND r.tx_hash = n.tx_hash
