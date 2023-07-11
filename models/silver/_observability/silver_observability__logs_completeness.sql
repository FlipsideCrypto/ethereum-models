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
receipts AS (
    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref("silver__receipts") }}
    WHERE
        block_number <= (
            SELECT
                end_block
            FROM
                block_range
        )
        AND ARRAY_SIZE(logs) > 0

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
logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash
    FROM
        {{ ref("silver__logs") }}
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
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            t.block_number,
            r.block_number
        ) AS block_number
    FROM
        receipts t full
        OUTER JOIN logs r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash
    WHERE
        r.tx_hash IS NULL
        OR t.tx_hash IS NULL
)
SELECT
    'event_logs' AS test_name,
    (
        SELECT
            MIN(block_number)
        FROM
            logs
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            logs
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            logs
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            logs
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            logs
    ) AS blocks_tested,
    (
        SELECT
            COUNT(*)
        FROM
            impacted_blocks
    ) AS blocks_impacted_count,
    (
        SELECT
            ARRAY_AGG(block_number) within GROUP (
                ORDER BY
                    block_number
            )
        FROM
            impacted_blocks
    ) AS blocks_impacted_array,
    CURRENT_TIMESTAMP() AS test_timestamp
