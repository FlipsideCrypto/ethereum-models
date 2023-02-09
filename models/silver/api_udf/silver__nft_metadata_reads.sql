{{ config (
    materialized = "view",
    unique_key = "collection_tokenid"
) }}

SELECT
    VALUE as full_metadata,
    VALUE :collectionTokenId as raw_collectionTokenId,
    VALUE :chain :: STRING AS chain,
    VALUE :collectionAddress :: STRING AS collectionAddress,
    VALUE :collectionName :: STRING AS collectionName,
    VALUE :collectionTokenId :: STRING AS collectionTokenId,
    VALUE :description :: STRING AS description,
    VALUE :imageUrl :: STRING AS imageUrl,
    VALUE :name :: STRING AS NAME,
    VALUE :network :: STRING AS network,
    VALUE :traits AS traits,
    CONCAT(collectionAddress, '-', collectionTokenId) as collection_tokenid,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__nft_metadata_details_reads') }},
    LATERAL FLATTEN(
        input => api_resp :data :result :tokens
    )


