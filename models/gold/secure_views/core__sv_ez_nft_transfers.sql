{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    *
FROM
    {{ ref('core__ez_nft_transfers') }}
