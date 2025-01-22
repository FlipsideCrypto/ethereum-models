{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
)
SELECT
    *
FROM
    {{ ref('core__fact_traces') }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
