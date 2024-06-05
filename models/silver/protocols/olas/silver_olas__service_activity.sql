{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH service_multisigs AS (

    SELECT
        DISTINCT multisig_address,
        id
    FROM
        {{ ref('silver_olas__create_service_multisigs') }}
),
decoded_evt AS (
    SELECT
        d.block_number,
        d.block_timestamp,
        d.tx_hash,
        d.origin_function_signature,
        d.origin_from_address,
        d.origin_to_address,
        d.contract_address,
        d.event_index,
        d.event_name,
        d.decoded_flat,
        s.multisig_address,
        s.id AS service_id,
        d._log_id,
        d._inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
        d
        INNER JOIN service_multisigs s
        ON d.origin_to_address = s.multisig_address
    WHERE
        d.tx_status = 'SUCCESS'

{% if is_incremental() %}
AND d._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    e.block_number,
    e.block_timestamp,
    e.tx_hash,
    e.origin_function_signature,
    e.origin_from_address,
    e.origin_to_address,
    e.contract_address,
    e.event_index,
    e.event_name,
    e.decoded_flat,
    e.multisig_address,
    e.service_id,
    m.name,
    m.description,
    e._log_id,
    e._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['e.tx_hash','e.event_index']
    ) }} AS service_activity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_evt e
    LEFT JOIN {{ ref('silver_olas__registry_metadata') }}
    m
    ON e.service_id = m.registry_id
WHERE
    m.contract_address = '0x48b6af7b12c71f09e2fc8af4855de4ff54e775ca' --Service Registry (AUTONOLAS-SERVICE-V1)
