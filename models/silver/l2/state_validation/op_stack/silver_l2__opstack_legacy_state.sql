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
)
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
    CONCAT(
        '0x',
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') [0] :: STRING
    ) AS batch_root,
    utils.udf_hex_to_int(
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') [1] :: STRING
    ) :: INTEGER AS batch_size,
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
    AND C.chain_category = 'op_stack'
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
