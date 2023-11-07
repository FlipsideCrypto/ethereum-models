{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_EZ_NFT_TRANSFERS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('nft__ez_nft_transfers') }}
