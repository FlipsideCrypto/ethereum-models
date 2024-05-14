{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'multichain' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        decoded_flat :"from" :: STRING AS from_address,
        decoded_flat :"receiver" :: STRING AS receiver,
        decoded_flat :"swapoutID" :: STRING AS swapoutID,
        TRY_TO_NUMBER(
            decoded_flat :"toChainID" :: STRING
        ) AS toChainID,
        decoded_flat :"token" :: STRING AS token,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x0d969ae475ff6fcaf0dcfa760d4d8607244e8d95e9bf426f8d5d69f9a3e525af'
        AND contract_address IN (
            '0x1633d66ca91ce4d81f63ea047b7b19beb92df7f3',
            '0x93251f98acb0c83904320737aec091bce287f8f5'
        )
        AND tx_status = 'SUCCESS'

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
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    NAME AS platform,
    LOWER(from_address) AS sender,
    LOWER(receiver) AS receiver,
    LOWER(receiver) AS destination_chain_receiver,
    amount,
    toChainID AS destination_chain_id,
    token AS token_address,
    swapoutID AS swapout_id,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
