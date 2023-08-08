{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
  "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SYNTHETIX',
    'PURPOSE': 'STAKING, DEFI' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    user_address,
    event_name,
    minted_amount,
    snx_balance,
    escrowed_snx_balance,
    sds_balance,
    snx_price,
    sds_price,
    account_c_ratio,
    target_c_ratio
FROM
    {{ ref('silver__synthetix_snx_staking') }}
