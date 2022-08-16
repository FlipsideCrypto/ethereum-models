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
    choices, 
    proposal_author, 
    proposal_title, 
    proposal_text, 
    space_id, 
    proposal_start_time, 
    proposal_end_time
FROM 
    {{ ref('silver__snapshot') }}