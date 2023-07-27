{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    eth_from_address,
    eth_to_address,
    amount,
    amount_usd
FROM
  {{ref('core__ez_eth_transfers')}}
  where block_timestamp::date between '2021-12-01' and '2021-12-31'