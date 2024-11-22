{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH requests AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'WithdrawRequestReceived' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS msgSender,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS recipient,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS requestId,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS shares_amount,
        (shares_amount / pow(10, 18)) :: FLOAT AS shares_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS ether_amount,
        (shares_amount / pow(10, 18)) :: FLOAT AS ether_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x5b2ce38527d3f69f0bf03c1a363829ba12d09551f8778a9b0b9e1285ec19721a' --WithdrawRequestReceived
        AND contract_address = '0x9f0491b32dbce587c50c4c43ab303b06478193a7' --TransparentUpgradeableProxy
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
    msgSender AS sender,
    recipient,
    requestId,
    ether_amount AS eth_amount,
    ether_amount_adj AS eth_amount_adj,
    shares_amount AS token_amount,
    shares_amount_adj AS token_amount_adj,
    '0xa35b1b31ce002fbf2058d22f30f95d405200a15b' AS token_address,
    'ETHx' AS token_symbol,
    'stader' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    requests
