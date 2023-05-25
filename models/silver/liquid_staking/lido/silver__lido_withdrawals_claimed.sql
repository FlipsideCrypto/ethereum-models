{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH claims AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topics [1] :: STRING
            )
        ) AS requestId,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS owner,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS receiver,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amountOfETH,
        (amountOfETH / pow(10, 18)) :: FLOAT AS amount_of_eth_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x6ad26c5e238e7d002799f9a5db07e81ef14e37386ae03496d7a7ef04713e145b' --WithdrawalClaimed
        AND contract_address = '0x889edc2edab5f40e902b864ad4d7ade8e412f9b1' --Lido: stETH Withdrawal NFT (unstETH)
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
    requestId AS request_id,
    owner,
    receiver,
    amountOfETH AS amount_of_eth,
    amount_of_eth_adj,
    _log_id,
    _inserted_timestamp
FROM
    claims
