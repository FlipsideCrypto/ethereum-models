{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH bridges AS (

    SELECT
        LOWER(contract_address) AS bridge_address,
        LOWER(contract_name) AS bridge_name,
        LOWER(blockchain) AS blockchain
    FROM
        {{ ref('silver_bridge__native_bridges_seed') }}
),
token_transfers AS (
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
        bridge_address,
        bridge_name,
        blockchain,
        raw_amount,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_token_transfers') }}
        t
        INNER JOIN bridges b
        ON t.to_address = b.bridge_address
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'

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
        t.from_address AS origin_from_address,
        t.to_address AS origin_to_address,
        t.origin_function_signature,
        et.from_address,
        et.to_address,
        bridge_address,
        bridge_name,
        blockchain,
        amount_precise_raw,
        et.ez_native_transfers_id AS _call_id,
        et.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_native_transfers') }}
        et
        INNER JOIN bridges b
        ON et.to_address = b.bridge_address
        LEFT JOIN {{ ref('core__fact_transactions') }}
        t USING(
            block_number,
            tx_hash
        )
    WHERE
        tx_hash NOT IN (
            SELECT
                DISTINCT tx_hash
            FROM
                token_transfers
        )

{% if is_incremental() %}
AND et.modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        'Transfer' AS event_name,
        bridge_address,
        bridge_name,
        from_address AS sender,
        to_address AS receiver,
        CASE 
            WHEN origin_from_address = '0x0000000000000000000000000000000000000000' THEN from_address
            ELSE origin_from_address
        END AS destination_chain_receiver,
        raw_amount AS amount_unadj,
        blockchain AS destination_chain,
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
        bridge_address,
        bridge_name,
        from_address AS sender,
        to_address AS receiver,
        CASE 
            WHEN origin_from_address = '0x0000000000000000000000000000000000000000' THEN from_address
            ELSE origin_from_address
        END AS destination_chain_receiver,
        amount_precise_raw AS amount_unadj,
        blockchain AS destination_chain,
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
    FINAL
WHERE
    origin_to_address IS NOT NULL
