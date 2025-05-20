{{ config (
    materialized = 'view',
    tags = ['test_silver','decoded_traces','recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_traces') }}
WHERE
    _inserted_timestamp >= DATEADD(DAY, -3, CURRENT_DATE())
