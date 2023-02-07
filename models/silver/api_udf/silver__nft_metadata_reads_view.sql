{{ config (
    materialized = "view",
    unique_key = "collection_page"
) }}

SELECT
    chain,
    collectionAddress,
    collectionName,
    collectionTokenId,
    description,
    imageUrl,
    NAME,
    network,
    traits,
    collection_page
FROM
    {{ ref('silver__nft_metadata_reads') }}


