{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH event_logs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        CASE
            WHEN topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5' THEN 'Deposit'
            WHEN topic_0 = '0x0e31f07bae79135368ff475cf6c7f6abb31e0fd731e03c18ad425bd9406cf0c0' THEN 'Lock'
            WHEN topic_0 = '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568' THEN 'Withdraw'
        END AS event_name,
        CASE
            WHEN contract_address = '0x7e01a500805f8a52fad229b3015ad130a332b7b3' THEN 'Voting Escrow OLAS (veOLAS)'
            WHEN contract_address = '0xb09ccf0dbf0c178806aaee28956c74bd66d21f73' THEN 'Burnable Locked OLAS (buOLAS)'
        END AS contract_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x7e01a500805f8a52fad229b3015ad130a332b7b3',
            --Voting Escrow OLAS (veOLAS)
            '0xb09ccf0dbf0c178806aaee28956c74bd66d21f73'
        ) --Burnable Locked OLAS (buOLAS)
        AND topic_0 IN (
            '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5',
            --Deposit (veOLAS)
            '0x0e31f07bae79135368ff475cf6c7f6abb31e0fd731e03c18ad425bd9406cf0c0',
            --Lock (buOLAS)
            '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568' --Withdraw (veOLAS and buOLAS)
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
),
veolas_deposit AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        event_name,
        contract_name,
        DATA,
        segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS account_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount_olas_unadj,
        amount_olas_unadj / pow(
            10,
            18
        ) :: FLOAT AS amount_olas_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS end_time_int,
        TO_TIMESTAMP(end_time_int) AS end_time_timestamp,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS start_time_int,
        TO_TIMESTAMP(start_time_int) AS start_time_timestamp,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS deposit_type,
        _log_id,
        _inserted_timestamp
    FROM
        event_logs
    WHERE
        event_name = 'Deposit'
        AND contract_name = 'Voting Escrow OLAS (veOLAS)'
),
buolas_lock AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        event_name,
        contract_name,
        DATA,
        segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS account_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount_olas_unadj,
        amount_olas_unadj / pow(
            10,
            18
        ) :: FLOAT AS amount_olas_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS end_time_int,
        TO_TIMESTAMP(end_time_int) AS end_time_timestamp,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS start_time_int,
        TO_TIMESTAMP(start_time_int) AS start_time_timestamp,
        NULL AS deposit_type,
        _log_id,
        _inserted_timestamp
    FROM
        event_logs
    WHERE
        event_name = 'Lock'
        AND contract_name = 'Burnable Locked OLAS (buOLAS)'
),
withdraw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        event_name,
        contract_name,
        DATA,
        segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS account_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount_olas_unadj,
        amount_olas_unadj / pow(
            10,
            18
        ) :: FLOAT AS amount_olas_adj,
        NULL AS end_time_int,
        NULL AS end_time_timestamp,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS start_time_int,
        TO_TIMESTAMP(start_time_int) AS start_time_timestamp,
        NULL AS deposit_type,
        _log_id,
        _inserted_timestamp
    FROM
        event_logs
    WHERE
        event_name = 'Withdraw'
),
all_evt AS (
    SELECT
        *
    FROM
        veolas_deposit
    UNION ALL
    SELECT
        *
    FROM
        buolas_lock
    UNION ALL
    SELECT
        *
    FROM
        withdraw
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    event_name,
    contract_name,
    DATA,
    segmented_data,
    account_address,
    amount_olas_unadj,
    amount_olas_adj,
    end_time_int,
    end_time_timestamp,
    start_time_int,
    start_time_timestamp,
    deposit_type,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS olas_locking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_evt
