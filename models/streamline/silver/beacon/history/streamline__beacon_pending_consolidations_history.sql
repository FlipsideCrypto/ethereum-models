{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"pending_consolidations",
        "sql_limit" :"1000000",
        "producer_batch_size" :"1000",
        "worker_batch_size" :"100",
        "async_concurrent_requests" :"10",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["data"]) }
    ),
    tags = ['streamline_beacon_history']
) }}

WITH to_do AS (
    SELECT
        slot_number
    FROM
        {{ ref("streamline__beacon_blocks") }}
    WHERE
        slot_number <= (
            SELECT 
                MIN(slot_number) 
            FROM 
                {{ ref("beacon_chain__fact_blocks") }} 
            WHERE 
                slot_timestamp >= DATEADD(hour, -12, SYSDATE())
        )
        and slot_number >= 11649025
    EXCEPT
    SELECT
        slot_number
    FROM
        {{ ref("streamline__complete_beacon_pending_consolidations") }}
    WHERE
        slot_number <= (
            SELECT 
                MIN(slot_number) 
            FROM 
                {{ ref("beacon_chain__fact_blocks") }} 
            WHERE 
                slot_timestamp >= DATEADD(hour, -12, SYSDATE())
        )
)   
SELECT
    slot_number,
    ROUND(slot_number, -3) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{service}/{Authentication}/eth/v1/beacon/states/' || slot_number || '/pending_consolidations',
        OBJECT_CONSTRUCT(
            'accept', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        {},
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS request
FROM
    to_do
ORDER BY
    slot_number DESC
LIMIT 1000000

