{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false
) }}

WITH look_back AS (

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
block_range AS (
    SELECT
        MAX(block_number) AS end_block,
        MIN(block_number) AS start_block
    FROM
        look_back
),
blocks AS (
    SELECT
        block_number,
        block_timestamp,
        LAG(
            block_number,
            1
        ) over (
            ORDER BY
                block_number ASC
        ) AS prev_BLOCK_NUMBER
    FROM
        {{ ref("silver__blocks") }}
    WHERE
        block_number <= (
            SELECT
                end_block
            FROM
                block_range
        )

{% if is_incremental() %}
AND (
    (
        block_number BETWEEN (
            SELECT
                start_block
            FROM
                block_range
        )
        AND (
            SELECT
                end_block
            FROM
                block_range
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        block_number >= 0
    {% else %}
        block_number >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        blocks_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array))
    {% endif %})
)
{% endif %}
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
