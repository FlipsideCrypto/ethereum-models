{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
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
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    from_address AS sender,
    amount,
    TIME,
    time_of_deposit,
    amount_adj,
    _log_id,
    _inserted_timestamp
FROM
    deposits
