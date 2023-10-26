{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curated']
) }}

WITH token_transfers AS (

    SELECT
        tr.block_number,
        tr.block_timestamp,
        tr.origin_function_signature,
        tr.origin_from_address,
        tr.origin_to_address,
        tr.tx_hash,
        event_index,
        tr.contract_address,
        tr.from_address,
        tr.to_address,
        raw_amount,
        SUBSTRING(
            input_data,
            74,
            1
        ) :: STRING AS destination_chain_id,
        _log_id,
        tr._inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
        tr
        INNER JOIN {{ ref('silver__transactions') }}
        tx
        ON tr.block_number = tx.block_number
        AND tr.tx_hash = tx.tx_hash
    WHERE
        tr.from_address <> '0x0000000000000000000000000000000000000000'
        AND tr.to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
        AND tr.origin_function_signature IN (
            '0x0f5287b0',
            -- TokenTransfer
            '0x9981509f'
        ) -- WrapAndTransfer

{% if is_incremental() %}
AND tr._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
native_transfers AS (
    SELECT
        et.block_number,
        et.block_timestamp,
        et.tx_hash,
        tx.from_address AS origin_from_address,
        tx.to_address AS origin_to_address,
        tx.origin_function_signature,
        et.from_address,
        et.to_address,
        eth_value,
        identifier,
        input,
        SUBSTRING(
            input_data,
            74,
            1
        ) :: STRING AS destination_chain_id,
        _call_id,
        et._inserted_timestamp
    FROM
        {{ ref('silver__eth_transfers') }}
        et
        INNER JOIN {{ ref('silver__transactions') }}
        tx
        ON et.block_number = tx.block_number
        AND et.tx_hash = tx.tx_hash
    WHERE
        et.to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
        AND tx.origin_function_signature IN (
            '0x0f5287b0',
            -- TokenTransfer
            '0x9981509f'
        ) -- WrapAndTransfer

{% if is_incremental() %}
AND et._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
all_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        'tokenTransfer' AS event_name,
        to_address AS bridge_address,
        from_address AS sender,
        to_address AS receiver,
        raw_amount AS amount_unadj,
        destination_chain_id,
        contract_address AS token_address,
        {{ dbt_utils.generate_surrogate_key(
            ['_log_id']
        ) }} AS _id,
        _inserted_timestamp
    FROM
        token_transfers
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        NULL AS event_index,
        'wrapAndTransfer' AS event_name,
        to_address AS bridge_address,
        from_address AS sender,
        to_address AS receiver,
        eth_value * pow(
            10,
            18
        ) AS amount_unadj,
        destination_chain_id,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_address,
        {{ dbt_utils.generate_surrogate_key(
            ['_call_id']
        ) }} AS _id,
        _inserted_timestamp
    FROM
        native_transfers
)
SELECT
    *
FROM
    all_transfers
WHERE
    origin_to_address IS NOT NULL
