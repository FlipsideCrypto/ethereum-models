{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"beacon_validators_v2",
        "sql_limit" :"10",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_beacon_realtime']
) }}

WITH to_do AS (

    SELECT
        block_number AS slot_number,
        state_id
    FROM
        {{ ref("_max_beacon_block_by_date") }}
    EXCEPT
    SELECT
        slot_number,
        state_id
    FROM
        {{ ref("streamline__complete_beacon_validators") }}
),
ready_slots AS (
    SELECT
        slot_number,
        state_id
    FROM
        to_do
    UNION
    SELECT
        slot_number,
        state_id
    FROM
        {{ ref("_missing_validators") }}
)
SELECT
    slot_number,
    state_id,
    ROUND(
        slot_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{service}/{Authentication}/eth/v1/beacon/states/' || state_id || '/validators',
        OBJECT_CONSTRUCT(
            'accept',
            'application/json'
        ),
        NULL,
        'vault/prod/ethereum/quicknode/beacon'
    ) AS request
FROM
    ready_slots
ORDER BY
    slot_number DESC
LIMIT
    2 --remove for prod
