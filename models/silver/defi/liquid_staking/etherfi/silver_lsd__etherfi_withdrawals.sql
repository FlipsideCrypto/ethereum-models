{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        'Transfer' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
<<<<<<< HEAD
        block_timestamp :: DATE >= '2023-07-01'
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Withdraw/Burn (Transfer)
        AND contract_address = LOWER('0x35fA164735182de50811E8e2E824cFb9B6118ac2') --ether.fi: eETH Token (eETH)
        AND to_address = '0x0000000000000000000000000000000000000000'
        AND tx_status = 'SUCCESS'
=======
        topics [0] :: STRING = '0xf45a04d08a70caa7eb4b747571305559ad9fdf4a093afd41506b35c8a306fa94' --Withdrawn
        AND l.contract_address = '0x7623e9dc0da6ff821ddb9ebaba794054e078f8c4' --Ether.fi Early Adopter Program (eETH LSD token not yet deployed)
        AND CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) = t.to_address
        AND l.tx_status = 'SUCCESS'
        AND t.trace_status = 'SUCCESS'
>>>>>>> origin/main

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
    eth_amount AS token_amount,
    eth_amount_adj AS token_amount_adj,
    LOWER(contract_address) AS token_address,
    'eETH' AS token_symbol,
    'etherfi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals
