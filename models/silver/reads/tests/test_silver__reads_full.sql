{{ config (
    materialized = 'view',
    tags = ['test_silver','reads','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__reads') }}
