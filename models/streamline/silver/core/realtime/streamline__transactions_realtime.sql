{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_transactions(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
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
        {{ ref("streamline__complete_transactions") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
)
SELECT
    id,
    block_number
FROM
    to_do
UNION
SELECT
    MD5(
        CAST(
            COALESCE(CAST(block_number AS text), '' :: STRING) AS text
        )
    ) AS id,
    block_number
FROM
    (
        SELECT
            block_number
        FROM
            {{ ref("_missing_txs") }}
        UNION
        SELECT
            block_number
        FROM
            {{ ref("_unconfirmed_blocks") }}
    )
ORDER BY
    block_number ASC
LIMIT
    300
