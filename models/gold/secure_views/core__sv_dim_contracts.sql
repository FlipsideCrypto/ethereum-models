{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_DIM_CONTRACTS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contracts') }}
