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
blocks_count AS (
    SELECT
        C.block_number,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__confirmed_blocks") }} C
        INNER JOIN block_range b
        ON C.block_number = b.block_number
        AND C.block_number >= (
            SELECT
                MIN(block_number)
            FROM
                all_blocks
        )
),
txs_count AS (
    SELECT
        t.block_number,
        block_timestamp,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__transactions") }}
        t
        INNER JOIN block_range b
        ON t.block_number = b.block_number
        AND t.block_number >= (
            SELECT
                MIN(block_number)
            FROM
                all_blocks
        )
),
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            b.block_number,
            t.block_number
        ) AS block_number
    FROM
        blocks_count b full
        OUTER JOIN txs_count t
        ON b.block_number = t.block_number
        AND b.block_hash = t.block_hash
        AND b.tx_hash = t.tx_hash
    WHERE
        (
            t.tx_hash IS NULL
            OR b.tx_hash IS NULL
        )
        AND t.block_number <= (
            SELECT
                MAX(block_number)
            FROM
                blocks_count
        )
)
SELECT
    'transactions' AS test_name,
    (
        SELECT
            MIN(block_number)
        FROM
            txs_count
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            txs_count
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            txs_count
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            txs_count
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            txs_count
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
