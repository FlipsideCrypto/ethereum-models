{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    _inserted_timestamp >= DATEADD(DAY, -3, CURRENT_DATE())
