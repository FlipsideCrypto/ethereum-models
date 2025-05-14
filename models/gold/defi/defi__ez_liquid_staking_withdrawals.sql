{{ config(
  materialized = 'view',
  persist_docs ={ "relation": true,
  "columns": true },
  meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'ANKR, COINBASE, CREAM, FRAX, LIDO, NODEDAO, ROCKETPOOL, SHAREDSTAKE, STADER, STAFI, UNIETH, ETHERFI, LIQUIDCOLLECTIVE, MANTLE, ORIGIN, STAKESTONE',
            'PURPOSE': 'LIQUID STAKING, LSD'
            }
        }
    },
  tags = ['gold','defi','liquid_staking','curated','ez']
) }}

SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  event_name,
  contract_address,
  recipient AS staker,
  platform,
  token_symbol,
  token_address,
  token_amount_unadj,
  token_amount_adj AS token_amount,
  ROUND(
        token_amount_usd,
        2
    ) AS token_amount_usd,
    eth_amount_unadj,
    eth_amount_adj AS eth_amount,
    ROUND(
        eth_amount_usd,
        2
    ) AS eth_amount_usd,
  COALESCE (
    complete_lsd_withdrawals_id,
    {{ dbt_utils.generate_surrogate_key(
      ['tx_hash', 'event_index']
    ) }}
  ) AS ez_liquid_staking_withdrawals_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  {{ ref('silver_lsd__complete_lsd_withdrawals') }}
