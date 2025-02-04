{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    blockchain,
    nft_address AS contract_address, --new column
    collection_name,
    tokenid AS token_id, --new column
    traits,
    tokenid_name AS token_id_name, --new column
    tokenid_description AS token_id_description, --new column
    tokenid_image_url AS token_id_image_url, --new column
    nft_address_tokenid AS nft_address_token_id, --new column
    COALESCE (
        nft_collection_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['nft_address','tokenid']
        ) }}
    ) AS dim_nft_collection_metadata_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    nft_address, --deprecate
    tokenid, --deprecate
    tokenid_name, --deprecate
    tokenid_description, --deprecate
    tokenid_image_url, --deprecate
    nft_address_tokenid --deprecate
FROM
    {{ ref('silver__nft_collection_metadata') }}
