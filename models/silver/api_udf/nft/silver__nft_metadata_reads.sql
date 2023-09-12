{{ config (
    materialized = 'incremental',
    unique_key = 'nft_address_tokenid'
) }}
{# full_refresh = false,
enabled = FALSE #}

SELECT
    nft_address,
    current_page,
    end_page,
    collection_page,
    VALUE,
    VALUE :collectionName :: STRING AS collection_name,
    VALUE :collectionTokenId :: STRING AS tokenid,
    VALUE :description :: STRING AS tokenid_description,
    VALUE :imageUrl :: STRING AS tokenid_image_url,
    VALUE :name :: STRING AS tokenid_name,
    VALUE :traits AS traits,
    CONCAT(
        nft_address,
        '-',
        tokenid
    ) AS nft_address_tokenid,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__nft_metadata_reads') }},
    LATERAL FLATTEN(
        input => api_resp :data :result :tokens
    )

{% if is_incremental() %}
WHERE
    nft_address_tokenid NOT IN (
        SELECT
            nft_address_tokenid
        FROM
            {{ this }}
    )
{% endif %}
