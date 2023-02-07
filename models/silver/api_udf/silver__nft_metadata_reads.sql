{{ config (
    materialized = "table",
    unique_key = "collection_page"
) }}

SELECT
    VALUE :chain :: STRING AS chain,
    VALUE :collectionAddress :: STRING AS collectionAddress,
    VALUE :collectionName :: STRING AS collectionName,
    VALUE :collectionTokenId :: INTEGER AS collectionTokenId,
    VALUE :description :: STRING AS description,
    VALUE :imageUrl :: STRING AS imageUrl,
    VALUE :name :: STRING AS NAME,
    VALUE :network :: STRING AS network,
    VALUE :traits AS traits,
    CONCAT(collectionAddress, '-', collectionTokenId) as collection_page
FROM
    {{ ref('bronze_api__nft_metadata_reads') }},
    LATERAL FLATTEN(
        input => api_resp :data :result :tokens
    )


