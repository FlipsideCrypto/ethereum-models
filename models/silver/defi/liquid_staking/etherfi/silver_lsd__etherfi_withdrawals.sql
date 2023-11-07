{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    enabled = false,
    tags = ['curated','reorg']
) }}
--disabled until launch of liquid staking derivative token

WITH withdrawals AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.tx_hash,
        event_index,
        'Withdrawn' AS event_name,
        l.contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        t.eth_value * pow(
            10,
            18
        ) AS amount,
        t.eth_value AS amount_adj,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__traces') }}
        t
        ON l.tx_hash = t.tx_hash
    WHERE
        topics [0] :: STRING = '0xf45a04d08a70caa7eb4b747571305559ad9fdf4a093afd41506b35c8a306fa94' --Withdrawn
        AND l.contract_address = '0x7623e9dc0da6ff821ddb9ebaba794054e078f8c4' --Ether.fi Early Adopter Program (eETH LSD token not yet deployed)
        AND CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) = t.to_address

{% if is_incremental() %}
AND l._inserted_timestamp >= (
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
    sender,
    sender AS recipient,
    amount AS eth_amount,
    amount_adj AS eth_amount_adj,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
