{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_olas','curated','olas','heal']
) }}

WITH registry_evt AS (

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
            WHEN topic_0 = '0x9169d45eacd63571e315a0504da919b7c89de505493e7b34051802dd0816a069' THEN 'CreateService'
            WHEN topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'Transfer'
        END AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x48b6af7b12c71f09e2fc8af4855de4ff54e775ca' --Service Registry (AUTONOLAS-SERVICE-V1)
        AND topic_0 IN (
            '0x9169d45eacd63571e315a0504da919b7c89de505493e7b34051802dd0816a069',
            --CreateService (for services)
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        )
        AND tx_succeeded

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
transfers AS (
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
        DATA,
        segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_3
            )
        ) AS id,
        _log_id,
        _inserted_timestamp
    FROM
        registry_evt
    WHERE
        event_name = 'Transfer'
),
multisigs AS (
    SELECT
        DISTINCT multisig_address,
        id,
        contract_address
    FROM
        {{ ref('silver_olas__create_service_multisigs') }}
        qualify(ROW_NUMBER() over (PARTITION BY multisig_address
    ORDER BY
        block_timestamp DESC)) = 1 --get latest service multisig address
),
services AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.origin_function_signature,
        r.origin_from_address,
        r.origin_to_address,
        r.contract_address,
        r.event_index,
        r.event_name,
        TRY_TO_NUMBER(utils.udf_hex_to_int(r.topic_1)) AS service_id,
        t.to_address AS owner_address,
        m.multisig_address,
        r._log_id,
        r._inserted_timestamp
    FROM
        registry_evt r
        LEFT JOIN transfers t
        ON r.tx_hash = t.tx_hash
        AND r.contract_address = t.contract_address
        AND service_id = t.id
        LEFT JOIN multisigs m
        ON r.contract_address = m.contract_address
        AND service_id = m.id
    WHERE
        r.event_name = 'CreateService'
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        t0.block_number,
        t0.block_timestamp,
        t0.tx_hash,
        t0.origin_function_signature,
        t0.origin_from_address,
        t0.origin_to_address,
        t0.contract_address,
        t0.event_index,
        t0.event_name,
        t0.service_id,
        t0.owner_address,
        m.multisig_address, --fill late-arriving or replace with current multisig
        t0._log_id,
        t0._inserted_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN multisigs m
        ON t0.contract_address = m.contract_address
        AND t0.service_id = m.id
    WHERE
        t0.block_number IN (
            SELECT
                t1.block_number
            FROM
                {{ this }}
                t1
            WHERE
                CONCAT(
                    COALESCE(
                        t1.multisig_address,
                        '0x'
                    ),
                    '-',
                    t1.service_id
                ) NOT IN (
                    SELECT
                        CONCAT(
                            multisig_address,
                            '-',
                            id
                        )
                    FROM
                        multisigs
                )
                AND t1.service_id IN (
                    SELECT
                        DISTINCT id
                    FROM
                        multisigs
                )
        )
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        services

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    *
FROM
    heal_model
{% endif %}
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
    event_name,
    owner_address,
    multisig_address,
    service_id,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS service_registration_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
