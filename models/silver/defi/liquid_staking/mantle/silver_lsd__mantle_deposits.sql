{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH stake AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Staked' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS staker,
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
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x1449c6dd7851abc30abf37f57715f492010519147cc2652fbc38202c18a6ee90' --Staked
        AND contract_address = LOWER('0xe3cBd06D7dadB3F4e6557bAb7EdD924CD1489E8f') --Mantle: LSP Staking

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
    staker AS sender,
    staker AS recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    LOWER('0xd5F7838F5C461fefF7FE49ea5ebaF7728bB0ADfa') AS token_address,
    'mETH' AS token_symbol,
    'mantle' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    stake
