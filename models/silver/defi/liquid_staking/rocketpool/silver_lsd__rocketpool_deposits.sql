{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH deposits AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'DepositReceived' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS TIME,
        TIME :: TIMESTAMP AS time_of_deposit,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x7aa1a8eb998c779420645fc14513bf058edb347d95c2fc2e6845bdc22f888631' --DepositReceived
        AND contract_address IN (
            '0xdd3f50f8a6cafbe9b31a427582963f465e745af8',
            '0x2cac916b2a963bf162f076c0a8a4a8200bcfbfb4',
            '0x4d05e3d48a938db4b7a9a59a802d5b45011bde58'
        ) --RocketDepositPool
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
mints AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'TokensMinted' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS eth_amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS TIME,
        TIME :: TIMESTAMP AS time_of_deposit,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x6155cfd0fd028b0ca77e8495a60cbe563e8bce8611f0aad6fedbdaafc05d44a2' --TokensMinted
        AND contract_address = '0xae78736cd615f374d3085123a210448e74fc6393' --rETH
        AND to_address IN (
            SELECT from_address
            FROM deposits
        )
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
    d.block_number,
    d.block_timestamp,
    d.origin_function_signature,
    d.origin_from_address,
    d.origin_to_address,
    d.tx_hash,
    d.event_index,
    d.event_name,
    d.contract_address,
    d.from_address AS sender,
    m.to_address AS recipient,
    d.amount AS eth_amount,
    d.amount_adj AS eth_amount_adj,
    m.amount AS token_amount,
    m.amount_adj AS token_amount_adj,
    m.contract_address AS token_address,
    'rETH' AS token_symbol,
    'rocketpool' AS platform,
    d.time_of_deposit,
    d._log_id,
    d._inserted_timestamp
FROM
    deposits d
    LEFT JOIN mints m
    ON d.tx_hash = m.tx_hash qualify (ROW_NUMBER() over (PARTITION BY d._log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1
