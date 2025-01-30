{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__transfers') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
