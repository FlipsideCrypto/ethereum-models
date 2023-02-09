{{ config (
    materialized = "view",
    unique_key = "collection_tokenid"
) }}

SELECT
    
    VALUE :chain :: STRING AS chain,
    lower(VALUE :collectionAddress :: STRING) AS collection_address,
    VALUE :collectionName :: STRING AS collection_name,
    VALUE :collectionTokenId :: STRING AS tokenId,
    VALUE :description :: STRING AS description,
    VALUE :imageUrl :: STRING AS imageUrl,
    VALUE :name :: STRING AS collection_name_tokenId,
    VALUE :network :: STRING AS network,
    VALUE :traits AS traits,
    CONCAT(collection_address, '-', tokenId) as collection_tokenid,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__nft_metadata_details_reads') }},
    LATERAL FLATTEN(
        input => api_resp :data :result :tokens
    )


