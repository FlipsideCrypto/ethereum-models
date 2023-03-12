{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'beacon_sync_committees', 'route', 'sync_committees', 'producer_batch_size', 5000,'producer_limit_size', 100000, 'worker_batch_size', 500, 'producer_batch_chunks_size', 25))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number AS slot_number
    FROM
        {{ ref("_max_beacon_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__beacon_sync_committees") }}
WHERE
    (
        slot_number >= (
            SELECT
                slot_number
            FROM
                last_3_days
        )
    )
    AND slot_number IS NOT NULL
    AND slot_number NOT IN (
        SELECT
            slot_number
        FROM
            {{ ref("streamline__complete_beacon_sync_committees") }}
        WHERE
            slot_number >= (
                SELECT
                    slot_number
                FROM
                    last_3_days
            )
    )
