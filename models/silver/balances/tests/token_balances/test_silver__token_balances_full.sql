{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__token_balances') }}
