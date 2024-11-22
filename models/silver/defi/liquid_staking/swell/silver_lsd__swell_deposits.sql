{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH deposits_t1 AS (

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
        AND tx_status = 'SUCCESS'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
),
deposits_t2 AS (

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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS referral,
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
        topics [0] :: STRING = '0xe28a9e1df63912c0c77b586c53595df741cbbc554d6831e40f1b5453199a9630' --ETHDepositReceived
        AND contract_address = '0xf951e335afb289353dc249e82926178eac7ded78' --swETH (swETH)
        AND tx_status = 'SUCCESS'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
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
    NULL AS referral,
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
    deposits_t1
UNION ALL
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
    referral,
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
    deposits_t2