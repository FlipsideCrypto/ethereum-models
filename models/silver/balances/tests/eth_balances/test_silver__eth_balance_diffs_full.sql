{{ config (
    materialized = 'view',
    tags = ['test_silver','balances','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__eth_balance_diffs') }}
