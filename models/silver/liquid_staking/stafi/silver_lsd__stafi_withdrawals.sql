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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS rethAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS ethAmount,
        (rethAmount / pow(10, 18)) :: FLOAT AS reth_amount_adj,
        (ethAmount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc7ccdcb2d25f572c6814e377dbb34ea4318a4b7d3cd890f5cfad699d75327c7c' --Unstake
        AND contract_address = '0x27d64dd9172e4b59a444817d30f7af8228f174cc' --StafiWithdrawProxy
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
    contract_address,
    from_address AS sender,
    from_address AS recipient,
    ethAmount AS eth_amount,
    eth_amount_adj,
    rethAmount AS token_amount,
    reth_amount_adj AS token_amount_adj,
    '0x9559aaa82d9649c7a7b220e7c461d2e74c9a3593' AS token_address,
    'rETH' AS token_symbol,
    'stafi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals
