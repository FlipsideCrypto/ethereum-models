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
txs AS (
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
receipts AS (
    SELECT
        r.block_number,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__receipts") }}
        r
        INNER JOIN block_range b
        ON r.block_number = b.block_number
        AND r.block_number >= (
            SELECT
                MIN(block_number)
            FROM
                all_blocks
        )
),
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            t.block_number,
            r.block_number
        ) AS block_number
    FROM
        txs t full
        OUTER JOIN receipts r
        ON t.block_number = r.block_number
        AND t.block_hash = r.block_hash
        AND t.tx_hash = r.tx_hash
    WHERE
        r.tx_hash IS NULL
        OR t.tx_hash IS NULL
)
SELECT
    'receipts' AS test_name,
    (
        SELECT
            MIN(block_number)
        FROM
            receipts
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            receipts
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            txs
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            txs
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            receipts
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
