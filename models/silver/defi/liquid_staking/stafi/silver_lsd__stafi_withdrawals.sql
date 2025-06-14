{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','liquid_staking','curated']
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
        'Unstake' AS event_name,
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
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xc7ccdcb2d25f572c6814e377dbb34ea4318a4b7d3cd890f5cfad699d75327c7c' --Unstake
        AND contract_address = '0x27d64dd9172e4b59a444817d30f7af8228f174cc' --StafiWithdrawProxy
        AND tx_succeeded
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
