{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH validation_addresses AS (

    SELECT
        chain,
        chain_category,
        validation_address,
        validation_type
    FROM
        {{ ref('silver_l2_validation__address_list') }}
)
SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    contract_address,
    '0x' || SUBSTR(
        topic_1,
        27
    ) :: STRING AS dispute_proxy_address,
    utils.udf_hex_to_int(
        topic_2
    ) :: INT AS game_type,
    '0x' || (
        topic_3
    ) :: STRING AS root_claim,
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
    AND C.validation_type = 'dispute_game'
WHERE
    block_timestamp :: DATE >= '2024-06-01'
    AND topic_0 = '0x5b565efe82411da98814f356d0e7bcb8f0219b8d970307c5afb4a6903a8b2e35' -- disputegamecreated event

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
