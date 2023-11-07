{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_EZ_DEX_SWAPS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('defi__ez_dex_swaps') }}
