{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_real_time']
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
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Withdraw/Burn (Transfer)
        AND contract_address = '0x898bad2774eb97cf6b94605677f43b41871410b1' --validator-Eth2 (vETH2)
        AND to_address = '0x0000000000000000000000000000000000000000'
        AND origin_to_address = '0xbca3b7b87dcb15f0efa66136bc0e4684a3e5da4d'
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
    eth_amount AS token_amount,
    eth_amount_adj AS token_amount_adj,
    contract_address AS token_address,
    'vETH2' AS token_symbol,
    'sharedstake' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals