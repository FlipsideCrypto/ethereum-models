{{ config (
    materialized = 'view',
    tags = ['test_silver','decoded_traces','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_traces') }}
