{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH withdrawals AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS ownerAddress,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS receiverAddress,
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
        ) AS isAETH,
        CASE
            WHEN isAETH = 1 THEN TRUE 
            ELSE FALSE
        END AS is_aeth,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc5130045b6f6c9e2944ccea448ad17c279db68237b8aa856ee12cbfaa25f7715' --PendingUnstake
        AND contract_address = '0x84db6ee82b7cf3b47e8f19270abde5718b936670' --ankr ETH2 Staking
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
    ownerAddress AS sender,
    receiverAddress AS recipient,
    amount AS eth_amount,
    amount_adj AS eth_amount_adj,
    is_aeth,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals