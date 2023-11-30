{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'MAKER, MKR',
                'PURPOSE': 'GOVERNANCE, DEFI'
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
    tx_status, 
    event_index, 
    voter, 
    polling_contract, 
    vote_option, 
    proposal_id,
    COALESCE (
        governance_votes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_governance_votes_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM 
    {{ ref('silver_maker__governance_votes') }}