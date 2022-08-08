{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    decimals,
    symbol,
    token_price,
    amount,
    amount_usd,
    has_decimal,
    has_price
FROM
  {{ref('core__ez_token_transfers')}}
  where block_timestamp::date between '2021-12-01' and '2021-12-31'