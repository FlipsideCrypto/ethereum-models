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
    tags = ['non_realtime']
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
    proposal_id
FROM 
    {{ ref('silver_maker__governance_votes') }}