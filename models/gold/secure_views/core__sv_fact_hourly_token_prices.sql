{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_FACT_HOURLY_TOKEN_PRICES', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('silver__prices') }}
