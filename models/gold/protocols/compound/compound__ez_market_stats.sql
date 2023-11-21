{{ config(
  materialized = 'view',
  persist_docs ={ "relation": true,
  "columns": true },
  meta={
      'database_tags':{
          'table': {
              'PROTOCOL': 'COMPOUND',
              'PURPOSE': 'DEFI'
          }
      }
  }
) }}
-- pull all ctoken addresses and corresponding name

SELECT
  block_number,
  block_timestamp,
  block_hour,
  contract_name,
  ctoken_address,
  underlying_contract,
  underlying_symbol,
  token_price,
  ctoken_price,
  reserves_token_amount,
  borrows_token_amount,
  supply_token_amount,
  supply_usd,
  reserves_usd,
  borrows_usd,
  comp_speed,
  supply_apy,
  borrow_apy,
  comp_price,
  comp_speed_usd,
  comp_apy_borrow,
  comp_apy_supply,
    COALESCE (
        comp_market_stats_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'ctoken_address']
        ) }}
    ) AS ez_market_stats_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
  {{ ref('silver__comp_market_stats') }}
