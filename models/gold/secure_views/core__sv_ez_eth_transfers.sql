{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_EZ_ETH_TRANSFERS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('core__ez_eth_transfers') }}
