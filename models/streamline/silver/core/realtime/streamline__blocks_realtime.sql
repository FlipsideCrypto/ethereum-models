{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_blocks(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
to_do AS (
    SELECT
        MD5(
            CAST(
                COALESCE(CAST(block_number AS text), '' :: STRING) AS text
            )
        ) AS id,
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        id,
        block_number
    FROM
        {{ ref("streamline__complete_blocks") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    id,
    block_number
FROM
    to_do
ORDER BY
    block_number ASC
LIMIT
    300
