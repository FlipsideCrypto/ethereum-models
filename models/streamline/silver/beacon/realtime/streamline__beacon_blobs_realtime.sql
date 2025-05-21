{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"beacon_blobs_v2",
        "sql_limit" :"1000",
        "producer_batch_size" :"30",
        "worker_batch_size" :"10",
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
        slot_number >= 8626178 -- EIP-4844 fork slot
    EXCEPT
    SELECT
        slot_number
    FROM
        {{ ref("streamline__complete_beacon_blobs") }}
),
ready_slots AS (
    SELECT
        slot_number
    FROM
        to_do
)
SELECT
    slot_number,
    ROUND(
        slot_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{service}/{Authentication}/eth/v1/beacon/blob_sidecars/' || slot_number,
        OBJECT_CONSTRUCT(
            'accept', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        {},
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS request
FROM
    ready_slots
ORDER BY
    slot_number DESC
limit 1000
