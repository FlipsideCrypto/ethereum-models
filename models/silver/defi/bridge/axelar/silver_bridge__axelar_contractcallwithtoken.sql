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
        'axelar' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        decoded_flat :"destinationChain" :: STRING AS destinationChain,
        LOWER(
            decoded_flat :"destinationContractAddress" :: STRING
        ) AS destinationContractAddress,
        decoded_flat :"payload" :: STRING AS payload,
        origin_from_address AS recipient,
        decoded_flat :"payloadHash" :: STRING AS payloadHash,
        decoded_flat :"sender" :: STRING AS sender,
        decoded_flat :"symbol" :: STRING AS symbol,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x7e50569d26be643bda7757722291ec66b1be66d8283474ae3fab5a98f878a7a2'
        AND contract_address = '0x4f4495243837681061c4743b74b3eedf548d56a5'
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
),
native_gas_paid AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'axelar' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        decoded_flat :"destinationChain" :: STRING AS destinationChain,
        LOWER(
            decoded_flat :"destinationAddress" :: STRING
        ) AS destinationAddress,
        TRY_TO_NUMBER(
            decoded_flat :"gasFeeAmount" :: STRING
        ) AS gasFeeAmount,
        decoded_flat :"payloadHash" :: STRING AS payloadHash,
        decoded_flat :"refundAddress" :: STRING AS refundAddress,
        decoded_flat :"sourceAddress" :: STRING AS sourceAddress,
        decoded_flat :"symbol" :: STRING AS symbol,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x999d431b58761213cf53af96262b67a069cbd963499fd8effd1e21556217b841'
        AND contract_address = '0x2d5d7d31f671f86c782533cc367f14109a082712'
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
),
transfers AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address AS token_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        from_address = '0xce16f69375520ab01377ce7b88f5ba8c48f8d666'
        AND to_address IN (
            '0x4f4495243837681061c4743b74b3eedf548d56a5',
            '0x0000000000000000000000000000000000000000'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.origin_function_signature,
        b.origin_from_address,
        b.origin_to_address,
        b.tx_hash,
        b.event_index,
        b.topic_0,
        b.event_name,
        b.event_removed,
        b.tx_status,
        b.contract_address AS bridge_address,
        b.name AS platform,
        b.origin_from_address AS sender,
        CASE
            WHEN b.recipient = '0x0000000000000000000000000000000000000000' THEN refundAddress
            ELSE b.recipient
        END AS receiver,
        CASE
            WHEN LOWER(
                b.destinationChain
            ) = 'avalanche' THEN 'avalanche c-chain'
            WHEN LOWER(
                b.destinationChain
            ) = 'binance' THEN 'bnb smart chain mainnet'
            WHEN LOWER(
                b.destinationChain
            ) = 'celo' THEN 'celo mainnet'
            WHEN LOWER(
                b.destinationChain
            ) = 'ethereum' THEN 'ethereum mainnet'
            WHEN LOWER(
                b.destinationChain
            ) = 'fantom' THEN 'fantom opera'
            WHEN LOWER(
                b.destinationChain
            ) = 'polygon' THEN 'polygon mainnet'
            ELSE LOWER(
                b.destinationChain
            )
        END AS destination_chain,
        b.destinationContractAddress AS destination_contract_address,
        CASE
            WHEN destination_chain IN (
                'arbitrum',
                'avalanche c-chain',
                'base',
                'bnb smart chain mainnet',
                'celo mainnet',
                'ethereum mainnet',
                'fantom opera',
                'filecoin',
                'kava',
                'linea',
                'mantle',
                'moonbeam',
                'optimism',
                'polygon mainnet',
                'scroll'
            ) THEN receiver
            ELSE destination_contract_address
        END AS destination_chain_receiver,
        b.amount,
        b.payload,
        b.payloadHash AS payload_hash,
        b.symbol AS token_symbol,
        t.token_address,
        b._log_id,
        b._inserted_timestamp
    FROM
        base_evt b
        INNER JOIN transfers t
        ON b.block_number = t.block_number
        AND b.tx_hash = t.tx_hash
        LEFT JOIN native_gas_paid n
        ON n.block_number = b.block_number
        AND n.tx_hash = b.tx_hash
)
SELECT
    *
FROM
    FINAL qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
