{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        regexp_substr_all(SUBSTR(input_data, 11, len(input_data)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS destination_chain_id,
        CONCAT(
            '0x',
            segmented_data [3] :: STRING
        ) AS recipient1,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS recipient2,
        LENGTH(
            REGEXP_SUBSTR(
                segmented_data [3] :: STRING,
                '^(0*)'
            )
        ) AS len,
        CASE
            WHEN len >= 24 THEN recipient2
            ELSE recipient1
        END AS destination_recipient_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS token,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING)) AS amount,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) AS arbiterFee,
        utils.udf_hex_to_int(
            segmented_data [5] :: STRING
        ) AS nonce,
        _log_id,
        tr._inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
        tr
        INNER JOIN {{ ref('core__fact_transactions') }}
        tx
        ON tr.block_number = tx.block_number
        AND tr.tx_hash = tx.tx_hash
    WHERE
        tr.from_address <> '0x0000000000000000000000000000000000000000'
        AND tr.to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
        AND tr.origin_function_signature = '0x0f5287b0' -- tokenTransfer
        AND destination_chain_id <> 0

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
        amount_precise_raw,
        identifier,
        regexp_substr_all(SUBSTR(input_data, 11, len(input_data)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS destination_chain_id,
        CONCAT(
            '0x',
            segmented_data [1] :: STRING
        ) AS recipient1,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS recipient2,
        LENGTH(
            REGEXP_SUBSTR(
                segmented_data [1] :: STRING,
                '^(0*)'
            )
        ) AS len,
        CASE
            WHEN len >= 24 THEN recipient2
            ELSE recipient1
        END AS destination_recipient_address,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS arbiterFee,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS nonce,
        _call_id,
        et._inserted_timestamp
    FROM
        {{ ref('silver__native_transfers') }}
        et
        INNER JOIN {{ ref('core__fact_transactions') }}
        tx
        ON et.block_number = tx.block_number
        AND et.tx_hash = tx.tx_hash
    WHERE
        et.to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
        AND tx.origin_function_signature = '0x9981509f' -- wrapAndTransfer
        AND destination_chain_id <> 0

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
        'Transfer' AS event_name,
        to_address AS bridge_address,
        from_address AS sender,
        to_address AS receiver,
        raw_amount AS amount_unadj,
        destination_chain_id,
        contract_address AS token_address,
        destination_recipient_address,
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
        NULL AS event_name,
        to_address AS bridge_address,
        from_address AS sender,
        to_address AS receiver,
        amount_precise_raw AS amount_unadj,
        destination_chain_id,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_address,
        destination_recipient_address,
        {{ dbt_utils.generate_surrogate_key(
            ['_call_id']
        ) }} AS _id,
        _inserted_timestamp
    FROM
        native_transfers
),
base_near AS (
    SELECT
        near_address,
        addr_encoded
    FROM
        {{ source(
            'crosschain_silver',
            'near_address_encoded'
        ) }}
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    event_name,
    'wormhole' AS platform,
    bridge_address,
    sender,
    receiver,
    amount_unadj,
    destination_chain_id,
    chain_name AS destination_chain,
    token_address,
    destination_recipient_address,
    --hex address on the destination chain, requires decoding for non-EVM - more info: https://docs.wormhole.com/wormhole/blockchain-environments/environments
    CASE 
        WHEN destination_chain = 'solana' THEN utils.udf_hex_to_base58(destination_recipient_address)
        WHEN destination_chain IN ('injective','sei') 
            THEN utils.udf_hex_to_bech32(destination_recipient_address,SUBSTR(destination_chain,1,3))
        WHEN destination_chain IN ('osmosis','xpla') 
            THEN utils.udf_hex_to_bech32(destination_recipient_address,SUBSTR(destination_chain,1,4))
        WHEN destination_chain IN ('terra','terra2','evmos') 
            THEN utils.udf_hex_to_bech32(destination_recipient_address,SUBSTR(destination_chain,1,5))
        WHEN destination_chain IN ('cosmoshub','kujira') 
            THEN utils.udf_hex_to_bech32(destination_recipient_address,SUBSTR(destination_chain,1,6))
        WHEN destination_chain IN ('near')
            THEN near_address
        WHEN destination_chain IN ('algorand')
            THEN utils.udf_hex_to_algorand(destination_recipient_address)
        WHEN destination_chain IN ('polygon')
            THEN SUBSTR(destination_recipient_address,1,42)
        ELSE destination_recipient_address 
    END AS destination_chain_receiver,
    _id,
    _inserted_timestamp
FROM
    all_transfers t
    LEFT JOIN {{ ref('silver_bridge__wormhole_chain_id_seed') }}
    s
    ON t.destination_chain_id :: STRING = s.wormhole_chain_id :: STRING
    LEFT JOIN base_near n
    ON t.destination_recipient_address = n.addr_encoded
WHERE
    origin_to_address IS NOT NULL qualify (ROW_NUMBER() over (PARTITION BY _id
ORDER BY
    _inserted_timestamp DESC)) = 1
