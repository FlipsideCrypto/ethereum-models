{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SNAPSHOT',
                'PURPOSE': 'GOVERNANCE'
            }
        }
    }
) }}

SELECT 
    id, 
    proposal_id, 
    LOWER(voter) AS voter, 
    vote_option, 
    voting_power, 
    vote_timestamp, 
    choices, 
    LOWER(proposal_author) AS proposal_author, 
    proposal_title, 
    proposal_text, 
    space_id,
    network, 
    delay, 
    quorum, 
    voting_period, 
    voting_type, 
    proposal_start_time, 
    proposal_end_time
FROM 
    {{ ref('silver__snapshot') }}