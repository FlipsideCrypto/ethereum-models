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
        ) AS destination_chain_symbol,
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
            decoded_flat :"tokenSourceAddress" :: STRING,
            '0+$',
            ''
        ) AS tokenSourceAddress,
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
        WHEN destination_chain_symbol = 'AURO' THEN 'aurora mainnet'
        WHEN destination_chain_symbol = 'AVA' THEN 'avalanche c-chain'
        WHEN destination_chain_symbol = 'BSC' THEN 'bnb smart chain mainnet'
        WHEN destination_chain_symbol = 'CELO' THEN 'celo mainnet'
        WHEN destination_chain_symbol = 'ETH' THEN 'ethereum mainnet'
        WHEN destination_chain_symbol = 'FTM' THEN 'fantom opera'
        WHEN destination_chain_symbol = 'HECO' THEN 'huobi eco chain mainnet'
        WHEN destination_chain_symbol = 'KLAY' THEN 'klaytn mainnet cypress'
        WHEN destination_chain_symbol = 'POL' THEN 'polygon mainnet'
        WHEN destination_chain_symbol = 'SOL' THEN 'solana'
        WHEN destination_chain_symbol = 'TRA' THEN 'terra'
        WHEN destination_chain_symbol = 'TEZ' THEN 'tezos'
        WHEN destination_chain_symbol = 'WAVE' THEN 'waves'
        ELSE LOWER(destination_chain_symbol)
    END AS destination_chain,
    CASE
        WHEN token_source = 'AURO' THEN 'aurora mainnet'
        WHEN token_source = 'AVA' THEN 'avalanche c-chain'
        WHEN token_source = 'BSC' THEN 'bnb smart chain-- mainnet'
        WHEN token_source = 'CELO' THEN 'celo mainnet'
        WHEN token_source = 'ETH' THEN 'ethereum mainnet'
        WHEN token_source = 'FTM' THEN 'fantom opera'
        WHEN token_source = 'HECO' THEN 'huobi eco chain mainnet'
        WHEN token_source = 'KLAY' THEN 'klaytn mainnet cypress'
        WHEN token_source = 'POL' THEN 'polygon mainnet'
        WHEN token_source = 'SOL' THEN 'solana'
        WHEN token_source = 'TRA' THEN 'terra'
        WHEN token_source = 'TEZ' THEN 'tezos'
        WHEN token_source = 'WAVE' THEN 'waves'
        ELSE LOWER(token_source)
    END AS source_chain,
    CASE
        WHEN destination_chain IN (
            'solana',
            'waves'
        ) THEN utils.udf_hex_to_base58(recipient)
        WHEN destination_chain ILIKE 'terra%' THEN utils.udf_hex_to_bech32(recipient, SUBSTR(destination_chain, 1, 5))
        WHEN destination_chain = 'tezos' THEN utils.udf_hex_to_tezos(CONCAT('0x', SUBSTR(recipient, 7, 40)), 'tz1')
        WHEN destination_chain = 'near' THEN utils.udf_hex_to_string(SUBSTR(recipient,3))
        WHEN destination_chain IN (
            'aurora mainnet',
            'avalanche c-chain',
            'bnb smart chain mainnet',
            'celo mainnet',
            'fantom opera',
            'fuse',
            'huobi eco chain mainnet',
            'klaytn mainnet cypress',
            'polygon mainnet'
        ) THEN SUBSTR(
            recipient,
            1,
            42
        )
        WHEN destination_chain = 'zzz' THEN origin_from_address
        ELSE recipient
    END AS destination_chain_receiver,
    tokenSourceAddress AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
WHERE
    source_chain = 'ethereum mainnet'
