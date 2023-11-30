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
  compound_version as version,
  _inserted_timestamp,
  _log_id,
  COALESCE (
        comp_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_deposits_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
  {{ ref('silver__comp_deposits') }}