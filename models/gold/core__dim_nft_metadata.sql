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
    commission_rate,
    contract_address,
    contract_name,
    created_at_block_id AS created_at_block_number,
    created_at_timestamp,
    created_at_tx_id AS created_at_tx_hash,
    creator_address,
    creator_name,
    image_url,
    project_name,
    token_id,
    token_metadata,
    token_metadata_uri,
    token_name
FROM
    {{ source(
        'ethereum_silver',
        'nft_metadata_legacy'
    ) }}
