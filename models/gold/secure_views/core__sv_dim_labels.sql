{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_DIM_LABELS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('silver__labels') }}
