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

deposits AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Deposited' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS receiver,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS token_amount,
        (token_amount / pow(10, 18)) :: FLOAT AS token_amount_adj,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS referrer,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} l 
    INNER JOIN vaults v ON l.contract_address = v.vault_address
    WHERE
        topics [0] :: STRING = '0x861a4138e41fb21c121a7dbb1053df465c837fc77380cc7226189a662281be2c' --Deposited/Stake
        AND tx_succeeded

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
    caller AS sender,
    receiver AS recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    '0xf1c9acdc66974dfb6decb12aa385b9cd01190e38' AS token_address,
    'osETH' AS token_symbol,
    'stakewise-v3' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    deposits
