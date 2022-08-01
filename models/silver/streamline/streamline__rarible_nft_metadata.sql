{{ config (
    materialized = "view",
) }}

SELECT
    VALUE :id :: STRING
FROM
    {{ source(
        'ethereum_external',
        'nft_collections_api'
    ) }}
WHERE
    provider = 'rarible'
EXCEPT
SELECT
    VALUE :collection :: STRING
FROM
    {{ source(
        'ethereum_external',
        'nft_metadata_api_rarible'
    ) }}
