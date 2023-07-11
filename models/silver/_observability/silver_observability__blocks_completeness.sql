{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false
) }}

WITH

{% if is_incremental() %}
min_failed_block AS (

    SELECT
        MIN(VALUE) - 1 AS block_number
    FROM
        (
            SELECT
                blocks_impacted_array
            FROM
                {{ this }}
                qualify ROW_NUMBER() over (
                    ORDER BY
                        test_timestamp DESC
                ) = 1
        ),
        LATERAL FLATTEN(
            input => blocks_impacted_array
        )
),
{% endif %}

look_back AS (
    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) BETWEEN 24
        AND 96
),
all_blocks AS (
    SELECT
        block_number
    FROM
        look_back

{% if is_incremental() %}
UNION
SELECT
    block_number
FROM
    min_failed_block
{% else %}
UNION
SELECT
    0 AS block_number
{% endif %}

{% if var('OBSERV_FULL_TEST') %}
UNION
SELECT
    0 AS block_number
{% endif %}
),
block_range AS (
    SELECT
        _id AS block_number
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id BETWEEN (
            SELECT
                MIN(block_number)
            FROM
                all_blocks
        )
        AND (
            SELECT
                MAX(block_number)
            FROM
                all_blocks
        )
),
blocks AS (
    SELECT
        l.block_number,
        block_timestamp,
        LAG(
            l.block_number,
            1
        ) over (
            ORDER BY
                l.block_number ASC
        ) AS prev_BLOCK_NUMBER
    FROM
        {{ ref("silver__blocks") }}
        l
        INNER JOIN block_range b
        ON l.block_number = b.block_number
        AND l.block_number >= (
            SELECT
                MIN(block_number)
            FROM
                all_blocks
        )
),
block_gen AS (
    SELECT
        _id AS block_number
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id BETWEEN (
            SELECT
                MIN(block_number)
            FROM
                blocks
        )
        AND (
            SELECT
                MAX(block_number)
            FROM
                blocks
        )
)
SELECT
    'blocks' AS test_name,
    MIN(
        b.block_number
    ) AS min_block,
    MAX(
        b.block_number
    ) AS max_block,
    MIN(
        b.block_timestamp
    ) AS min_block_timestamp,
    MAX(
        b.block_timestamp
    ) AS max_block_timestamp,
    COUNT(1) AS blocks_tested,
    COUNT(
        CASE
            WHEN C.block_number IS NOT NULL THEN A.block_number
        END
    ) AS blocks_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN C.block_number IS NOT NULL THEN A.block_number
        END
    ) within GROUP (
        ORDER BY
            A.block_number
    ) AS blocks_impacted_array,
    CURRENT_TIMESTAMP AS test_timestamp
FROM
    block_gen A
    LEFT JOIN blocks b
    ON A.block_number = b.block_number
    LEFT JOIN blocks C
    ON A.block_number > C.prev_block_number
    AND A.block_number < C.block_number
    AND C.block_number - C.prev_block_number <> 1
WHERE
    COALESCE(
        b.block_number,
        C.block_number
    ) IS NOT NULL
