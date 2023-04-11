{{ config (
    materialized = "incremental",
    unique_key = "collection_tokenid",
    enabled = false
) }}

WITH collection_without_traits AS (
    SELECT 
        nft_address as collection_address,
        collection_name,
        chain,
        network,
        '0' as tokenId, 
        null as description,
        null as imageUrl,
        null as collection_name_tokenId,
        null as traits,
        CONCAT(collection_address, '-', tokenId) as collection_tokenid,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__nft_metadata_pages_reads') }}
    WHERE  
        traits_value NOT LIKE '%trait_type%'

    {% if is_incremental() %}
    AND collection_tokenid NOT IN (
        SELECT
            collection_tokenid
        FROM
            {{ this }}
    )
    {% endif %} 
),

collection_with_traits AS (
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

    {% if is_incremental() %}
    WHERE collection_tokenid NOT IN (
        SELECT
            collection_tokenid
        FROM
            {{ this }}
    )
    {% endif %} 
)

SELECT
    chain,
    network, 
    collection_address, 
    collection_name, 
    tokenId,
    traits, 
    description,
    imageUrl, 
    collection_name_tokenId,
    collection_tokenid,
    _inserted_timestamp
FROM 
    collection_without_traits

UNION ALL 

SELECT
    chain,
    network, 
    collection_address, 
    collection_name, 
    tokenId,
    traits, 
    description,
    imageUrl, 
    collection_name_tokenId,
    collection_tokenid,
    _inserted_timestamp
FROM 
    collection_with_traits

