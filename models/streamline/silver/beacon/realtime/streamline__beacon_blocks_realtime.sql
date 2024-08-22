{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"beacon_blocks_v2",
        "sql_limit" :"100000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"10000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["data"]) }
    ),
    tags = ['streamline_beacon_realtime']
) }}

WITH to_do AS (

    SELECT
        slot_number
    FROM
        {{ ref("streamline__beacon_blocks") }}
    WHERE
        slot_number > 5000000
        AND slot_number IS NOT NULL
    EXCEPT
    SELECT
        slot_number
    FROM
        {{ ref("streamline__complete_beacon_blocks") }}
    WHERE
        slot_number > 5000000
),
ready_slots AS (
    SELECT
        slot_number
    FROM
        to_do
    UNION
    SELECT
        slot_number
    FROM
        {{ ref("_missing_withdrawals") }}
)
SELECT
    slot_number,
    ROUND(
        slot_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{service}/{Authentication}/eth/v2/beacon/blocks/' || slot_number,
        OBJECT_CONSTRUCT(
            'accept',
            'application/json'
        ),
        NULL,
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS request
FROM
    ready_slots
ORDER BY
    slot_number DESC
