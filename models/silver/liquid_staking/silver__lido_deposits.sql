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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS referral,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x96a25c8ce0baabc1fdefd93e9ed25d8e092a3332f3aa9a41722b5697231d1d1a' --Submitted
        AND contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' --Lido stETH Token (stETH)
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
    sender,
    referral AS referral_address,
    amount,
    amount_adj,
    _log_id,
    _inserted_timestamp
FROM
    deposits
