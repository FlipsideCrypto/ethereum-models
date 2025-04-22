{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH validation_addresses AS (

    SELECT
        chain,
        chain_category,
        validation_address,
        validation_type
    FROM
        {{ ref('silver_l2__validation_address') }}
),
base AS (
    SELECT
        block_number,
        block_timestamp,
        event_index,
        tx_hash,
        origin_from_address,
        origin_to_address,
        contract_address,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS batch_index,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        CONCAT(
            '0x',
            part [0] :: STRING
        ) AS batch_root,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INTEGER AS batch_size,
        utils.udf_hex_to_int(
            part [2] :: STRING
        ) :: INTEGER AS prev_total_elements,
        utils.udf_hex_to_int(
            part [3] :: STRING
        ) :: INTEGER / 32 AS extra_data_offset,
        utils.udf_hex_to_int(
            part [extra_data_offset] :: STRING
        ) :: INTEGER * 2 AS extra_data_length,
        SUBSTR(DATA, (3 + (64 * (extra_data_offset + 1))), extra_data_length) AS extra_data,
        OBJECT_CONSTRUCT(
            'batch_root',
            batch_root,
            'batch_size',
            batch_size,
            'prev_total_elements',
            prev_total_elements,
            'extra_data',
            extra_data
        ) AS validation_data_json,
        chain,
        chain_category,
        validation_address,
        validation_type,
        inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        e
        INNER JOIN validation_addresses C
        ON e.contract_address = C.validation_address
        AND C.validation_type = 'legacy_state'
    WHERE
        block_timestamp :: DATE >= '2021-11-01'
        AND topic_0 = '0x16be4c5129a4e03cf3350262e181dc02ddfb4a6008d925368c0899fcd97ca9c5' -- statebatchappended event

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address,
    batch_index,
    batch_root,
    batch_size,
    prev_total_elements,
    extra_data_offset,
    extra_data_length,
    extra_data,
    validation_data_json,
    chain,
    chain_category,
    validation_address,
    validation_type,
    inserted_timestamp
FROM
    base
