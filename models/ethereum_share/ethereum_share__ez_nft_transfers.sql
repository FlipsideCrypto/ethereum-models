{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    event_type,
    nft_address,
    project_name,
    nft_from_address,
    nft_to_address,
    tokenId,
    token_metadata,
    erc1155_value
FROM
  {{ref('core__ez_nft_transfers')}}
  where block_timestamp::date between '2021-12-01' and '2021-12-31'