{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
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
        'ETHDepositReceived' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
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
        ) AS swETHMinted,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS newTotalETHDeposited,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        (swETHMinted / pow(10, 18)) :: FLOAT AS sweth_minted_adj,
        (newTotalETHDeposited / pow(10, 18)) :: FLOAT AS new_total_eth_deposited_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xcb2ce03599937ff3d73e67e71a0f37013a6d3b697487823e37bc94da69483986' --ETHDepositReceived
        AND contract_address = '0xf951e335afb289353dc249e82926178eac7ded78' --swETH (swETH)
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
    amount AS eth_amount,
    amount_adj AS eth_amount_adj,
    swETHMinted AS token_amount,
    sweth_minted_adj AS token_amount_adj,
    contract_address AS token_address,
    'swETH' AS token_symbol,
    'swell' AS platform,
    newTotalETHDeposited AS total_eth_deposited,
    new_total_eth_deposited_adj AS total_eth_deposited_adj,
    _log_id,
    _inserted_timestamp
FROM
    deposits