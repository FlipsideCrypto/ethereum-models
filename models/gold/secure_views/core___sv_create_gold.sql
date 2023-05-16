{{ config(
    materialized = 'view',
    secure = true,
) }}
SELECT *
FROM {{ ref("core___create_gold")}}