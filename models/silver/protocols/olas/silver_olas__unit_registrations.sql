{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
            WHEN topic_0 = '0x97587a61bb0b1b9b24e5325039519ae27f44ca15ef278df10f4ab0195205c29c' THEN 'CreateUnit'
            WHEN topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'Transfer'
        END AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x15bd56669f57192a97df41a2aa8f4403e9491776',
            --Component Registry (AUTONOLAS-COMPONENT-V1)
            '0x2f1f7d38e4772884b88f3ecd8b6b9facdc319112' --Agent Registry (AUTONOLAS-AGENT-V1)
        )
        AND topics [0] :: STRING IN (
            '0x97587a61bb0b1b9b24e5325039519ae27f44ca15ef278df10f4ab0195205c29c',
            --CreateUnit (for both agents and components)
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
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
units AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.origin_function_signature,
        r.origin_from_address,
        r.origin_to_address,
        r.contract_address,
        r.event_index,
        r.topic_0,
        r.topic_1,
        r.topic_2,
        r.topic_3,
        r.event_name,
        r.data,
        r.segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                r.segmented_data [0] :: STRING
            )
        ) AS unit_id,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                r.segmented_data [1] :: STRING
            )
        ) AS u_type,
        CONCAT(
            '0x',
            r.segmented_data [2] :: STRING
        ) AS unit_hash,
        t.from_address,
        t.to_address AS owner_address,
        r._log_id,
        r._inserted_timestamp
    FROM
        registry_evt r
        LEFT JOIN transfers t
        ON r.tx_hash = t.tx_hash
        AND r.contract_address = t.contract_address
        AND unit_id = t.id
    WHERE
        r.event_name = 'CreateUnit'
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
    unit_id,
    u_type,
    CASE
        WHEN u_type = 0 THEN 'component'
        WHEN u_type = 1 THEN 'agent'
    END AS unit_type,
    unit_hash,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS unit_registration_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    units qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
