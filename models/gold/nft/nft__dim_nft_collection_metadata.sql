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
    collection_name,
    tokenid,
    traits,
    tokenid_name,
    tokenid_description,
    tokenid_image_url,
    nft_address_tokenid

FROM
    {{ ref('silver__nft_collection_metadata') }}
