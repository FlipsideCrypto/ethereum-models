{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT 
    id, 
    proposal_id, 
    voter, 
    vote_option, 
    voting_power, 
    vote_timestamp, 
    choices, 
    proposal_author, 
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