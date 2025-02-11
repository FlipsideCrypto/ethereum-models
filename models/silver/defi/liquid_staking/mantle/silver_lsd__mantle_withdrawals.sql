{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH unstake AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'UnstakeRequestClaimed' AS event_name,
        contract_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)))
        ) AS id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS requester,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS token_amount,
        (token_amount / pow(10, 18)) :: FLOAT AS token_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS cumulative_eth_requested,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS block_number_requested,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x5de6f9e4fdf1b740a7ba3b485303743eec250be281a2dd4df046c7fcecbdb04d' --UnstakeRequestClaimed
        AND contract_address = LOWER('0x38fDF7b489316e03eD8754ad339cb5c4483FDcf9') --Mantle: Unstake Requests Manager

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
    requester AS sender,
    requester AS recipient,
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
    unstake
