{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_EZ_NFT_SALES', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('core__ez_nft_sales') }}
