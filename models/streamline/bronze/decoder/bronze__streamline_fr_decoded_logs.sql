{{ config (
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('bronze__streamline_fr_decoded_logs_v2') }}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__streamline_fr_decoded_logs_v1') }}
