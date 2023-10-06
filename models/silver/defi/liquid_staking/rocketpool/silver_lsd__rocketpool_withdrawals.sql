{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH burns AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Burn' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS TIME,
        TIME :: TIMESTAMP AS time_of_burn,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x19783b34589160c168487dc7f9c51ae0bcefe67a47d6708fba90f6ce0366d3d1' --Burn
        AND contract_address = '0xae78736cd615f374d3085123a210448e74fc6393' --rETH

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    from_address AS sender,
    from_address AS recipient,
    eth_amount AS eth_amount,
    eth_amount_adj AS eth_amount_adj,
    amount AS token_amount,
    amount_adj AS token_amount_adj,
    contract_address AS token_address,
    'rETH' AS token_symbol,
    'rocketpool' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    burns
