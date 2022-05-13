{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_FACT_BLOCKS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('core__fact_blocks') }}
