{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"beacon_validators_v2",
        "sql_limit" :"2",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["data"]) }
    ),
    tags = ['streamline_beacon_history']
) }}

WITH to_do AS (

    SELECT
        slot_number,
        state_id
    FROM
        {{ ref("_premerge_max_daily_slots") }}
    EXCEPT
    SELECT
        slot_number,
        state_id
    FROM
        {{ ref("streamline__beacon_validators_complete") }}
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
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS request
FROM
    to_do
ORDER BY
    slot_number DESC
