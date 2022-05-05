{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contracts') }}
