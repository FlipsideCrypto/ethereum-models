{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    commission_rate,
    contract_address,
    contract_name,
    created_at_block_id,
    created_at_timestamp,
    created_at_tx_id,
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
        'flipside_gold_ethereum',
        'nft_metadata'
    ) }}
