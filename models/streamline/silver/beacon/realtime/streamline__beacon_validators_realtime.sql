{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"beacon_validators_v2",
        "sql_limit" :"10",
        "producer_batch_size" :"1",
        "worker_batch_size" :"1",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["data"]) }
    ),
    tags = ['streamline_beacon_realtime']
) }}

{# WITH to_do AS (

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
    WHERE _inserted_timestamp ::DATE < '2024-07-20' --remove for prod
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
) #}
SELECT
    3598 AS slot_number,
    '0x5173269ecb7322babf58778df4ddbf92d9fcb2b8f6546c94cfce829fe2a9cb1f' AS state_id,
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
{# FROM
    ready_slots
ORDER BY
    slot_number DESC
LIMIT
    3 --remove for prod #}
