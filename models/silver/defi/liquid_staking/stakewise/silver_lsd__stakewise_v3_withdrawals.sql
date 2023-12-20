{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH vaults AS (

    SELECT 
        vault_address
    FROM
        {{ ref('silver_lsd__stakewise_v3_vaults') }}
),

withdrawals AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'ExitedAssetsClaimed' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS receiver,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS prevPositionTicket,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS newPositionTicket,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }} l 
    INNER JOIN vaults v ON l.contract_address = v.vault_address
    WHERE
        topics [0] :: STRING = '0xeb3b05c070c24f667611fdb3ff75fe007d42401c573aed8d8faca95fd00ccb56' --ExitedAssetsClaimed/Unstake

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
    origin_from_address AS sender,
    receiver AS recipient,
    eth_amount,
    eth_amount_adj,
    eth_amount AS token_amount,
    eth_amount_adj AS token_amount_adj,
    '0xf1c9acdc66974dfb6decb12aa385b9cd01190e38' AS token_address,
    'osETH' AS token_symbol,
    'stakewise-v3' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals
