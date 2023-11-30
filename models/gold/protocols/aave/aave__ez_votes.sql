{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
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
  governance_contract,
  proposal_id,
  support,
  voting_power,
  voter,
  tx_hash,
  blockchain,
  _log_id,
  _inserted_timestamp,
    COALESCE (
        aave_votes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_votes_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
   {{ ref('silver__aave_votes') }}
