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
  _log_id,
    COALESCE (
        comp_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_liquidations_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
  {{ ref('silver__comp_liquidations') }}
