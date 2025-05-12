{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    blockchain,
    nft_address AS contract_address, 
    collection_name,
    tokenid AS token_id, 
    traits,
    tokenid_name AS token_id_name, 
    tokenid_description AS token_id_description, 
    tokenid_image_url AS token_id_image_url, 
    nft_address_tokenid AS nft_address_token_id, 
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
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_collection_metadata') }}
