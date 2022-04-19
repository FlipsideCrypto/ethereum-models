{{ config (
    materialized = 'view'
) }}

SELECT
    DISTINCT contract_address
FROM
    {{ ref('silver__seed_nft_metadata_upload') }}
EXCEPT
SELECT
    DISTINCT contract_address
FROM
    {{ source(
        'ethereum_external',
        'nft_metadata_api'
    ) }}
