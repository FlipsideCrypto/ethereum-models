{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
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
        'allbridge' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        utils.udf_hex_to_string(
            SUBSTRING(
                decoded_flat :"destination" :: STRING,
                3
            )
        ) AS destination_chain,
        decoded_flat :"lockId" :: STRING AS lockId,
        decoded_flat :"recipient" :: STRING AS recipient,
        decoded_flat :"sender" :: STRING AS sender,
        utils.udf_hex_to_string(
            SUBSTRING(
                decoded_flat :"tokenSource" :: STRING,
                3
            )
        ) AS token_source,
        REGEXP_REPLACE(
            decoded_flat :"tokenSourceAddress" :: STRING, '0+$', '') AS tokenSourceAddress,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x884a8def17f0d5bbb3fef53f3136b5320c9b39f75afb8985eeab9ea1153ee56d'
        AND contract_address = '0xbbbd1bbb4f9b936c3604906d7592a644071de884'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
HAVING LENGTH(tokenSourceAddress) = 42
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
    sender,
    recipient AS receiver,
    amount,
    lockId AS lock_id,
    CASE 
        WHEN destination_chain = 'AURO' THEN 'aurora'
        WHEN destination_chain = 'AVA' THEN 'avalanche'
        WHEN destination_chain = 'BSC' THEN 'binance'
        WHEN destination_chain = 'CELO' THEN 'celo'
        WHEN destination_chain = 'ETH' THEN 'ethereum'
        WHEN destination_chain = 'FTM' THEN 'fantom'
        WHEN destination_chain = 'HECO' THEN 'huobi eco chain'
        WHEN destination_chain = 'KLAY' THEN 'klaytn'
        WHEN destination_chain = 'POL' THEN 'polygon'
        WHEN destination_chain = 'SOL' THEN 'solana'
        WHEN destination_chain = 'TRA' THEN 'terra'
        WHEN destination_chain = 'TEZ' THEN 'tezos'
        WHEN destination_chain = 'WAVE' THEN 'waves'
        ELSE LOWER(destination_chain) 
    END AS destination_chain,
    CASE 
        WHEN token_source = 'AURO' THEN 'aurora'
        WHEN token_source = 'AVA' THEN 'avalanche'
        WHEN token_source = 'BSC' THEN 'binance'
        WHEN token_source = 'CELO' THEN 'celo'
        WHEN token_source = 'ETH' THEN 'ethereum'
        WHEN token_source = 'FTM' THEN 'fantom'
        WHEN token_source = 'HECO' THEN 'huobi eco chain'
        WHEN token_source = 'KLAY' THEN 'klaytn'
        WHEN token_source = 'POL' THEN 'polygon'
        WHEN token_source = 'SOL' THEN 'solana'
        WHEN token_source = 'TRA' THEN 'terra'
        WHEN token_source = 'TEZ' THEN 'tezos'
        WHEN token_source = 'WAVE' THEN 'waves'
        ELSE LOWER(token_source)
    END AS source_chain,
    tokenSourceAddress AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
WHERE source_chain = 'ethereum'
