{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true }
) }}
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  borrower,
  ctoken,
  ctoken_symbol,
  liquidator,
  ctokens_seized,
  collateral_token,
  collateral_symbol,
  liquidation_amount,
  liquidation_amount_usd,
  liquidation_contract_address,
  liquidation_contract_symbol,
  compound_version as version,
  _inserted_timestamp,
  _log_id
FROM
  {{ ref('silver__comp_liquidations') }}
