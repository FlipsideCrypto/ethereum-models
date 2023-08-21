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
    *
FROM
    {{ ref('nft__dim_nft_metadata') }}
