{{ config (
    materialized = 'view',
    tags = ['test_silver','balances','recent_test']
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
)
SELECT
    *
FROM
    {{ ref('silver__eth_balance_diffs') }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
