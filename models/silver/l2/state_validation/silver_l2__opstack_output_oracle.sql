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
        {{ ref('silver_l2__validation_address') }}
)
SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    contract_address,
    '0x' || topic_1 :: STRING AS output_root,
    utils.udf_hex_to_int(
        topic_2
    ) :: INT AS l2_output_index,
    utils.udf_hex_to_int(
        topic_3
    ) :: INT AS l2_block_number,
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
    AND C.validation_type = 'output_oracle'
WHERE
    block_timestamp :: DATE >= '2023-03-01'
    AND topic_0 = '0xa7aaf2512769da4e444e3de247be2564225c2e7a8f74cfe528e46e17d24868e2' -- outputproposed event

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
