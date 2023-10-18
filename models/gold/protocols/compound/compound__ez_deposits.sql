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
  ctoken,
  ctoken_symbol,
  issued_ctokens,
  supplied_base_asset,
  supplied_base_asset_usd,
  supplied_contract_addr,
  supplied_symbol,
  supplier,
  compound_version
FROM
  {{ ref('silver__comp_deposits') }}