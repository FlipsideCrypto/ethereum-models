{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'NFT'
            }
        }
    }
) }}

SELECT
    blockchain,
    nft_address,
    nft_address AS contract_address,
    collection_name,
    tokenid,
    tokenid AS token_id,
    traits,
    tokenid_name,
    tokenid_description,
    tokenid_image_url,
    nft_address_tokenid,
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
