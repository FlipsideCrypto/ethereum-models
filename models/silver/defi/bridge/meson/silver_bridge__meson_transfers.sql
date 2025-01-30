{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH token_transfers AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        from_address,
        to_address,
        raw_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'
        AND to_address = '0x25ab3efd52e6470681ce037cd546dc60726948d3'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
        et.to_address = '0x25ab3efd52e6470681ce037cd546dc60726948d3'

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
        NULL AS event_name,
        to_address AS bridge_address,
        from_address AS sender,
        to_address AS receiver,
        amount_precise_raw AS amount_unadj,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_address,
        {{ dbt_utils.generate_surrogate_key(
            ['_call_id']
        ) }} AS _id,
        _inserted_timestamp
    FROM
        native_transfers
),
dst_info AS (
    SELECT
        block_number,
        tx_hash,
        topics [1] :: STRING AS encoded_data,
        SUBSTR(RIGHT(encoded_data, 12), 1, 4) AS destination_chain_id,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x25ab3efd52e6470681ce037cd546dc60726948d3'
        AND topics [0] :: STRING = '0x5ce4019f772fda6cb703b26bce3ec3006eb36b73f1d3a0eb441213317d9f5e9d'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '16 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    t.block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    t.tx_hash,
    event_index,
    event_name,
    'meson' AS platform,
    bridge_address,
    sender,
    receiver,
    CASE 
        WHEN origin_from_address = '0x0000000000000000000000000000000000000000' THEN sender
        ELSE origin_from_address
    END AS destination_chain_receiver,
    amount_unadj,
    destination_chain_id,
    COALESCE(LOWER(chain),'other') AS destination_chain,
    token_address,
    _id,
    t._inserted_timestamp
FROM
    all_transfers t
    INNER JOIN dst_info d
    ON t.tx_hash = d.tx_hash
    AND t.block_number = d.block_number
    LEFT JOIN {{ ref('silver_bridge__meson_chain_id_seed') }}
    s
    ON d.destination_chain_id :: STRING = RIGHT(
        s.short_coin_type,
        4
    ) :: STRING
WHERE
    origin_to_address IS NOT NULL qualify (ROW_NUMBER() over (PARTITION BY _id
ORDER BY
    t._inserted_timestamp DESC)) = 1
