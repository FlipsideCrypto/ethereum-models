{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert'
) }}

WITH proposals AS (
    SELECT
        proposal_id,
        ipfs,
        choices,
        proposal_author,
        proposal_title,
        proposal_text,
        space_id,
        network,
        proposal_start_time,
        proposal_end_time,
        _inserted_timestamp
    FROM {{ ref('bronze_api__snapshot_proposals') }}

),  
votes AS ( 
    SELECT
        id,
        ipfs,
        proposal_id,
        voter,
        voting_power,
        vote_timestamp,
        vote_option,
        _inserted_timestamp
    FROM {{ ref('bronze_api__snapshot_votes') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
    )
{% endif %}
), 
networks AS (
    SELECT 
        name AS network,
        chainid :: STRING AS chain_id
    FROM 
        {{ source( 
            'ethereum_silver',
            'evm_chains_20221212'
        ) }}
), 
voting_strategy AS (
    SELECT 
        LTRIM(name, '#/') AS name, 
        delay, 
        quorum, 
        voting_period, 
        voting_type
    FROM 
        {{ source( 
            'ethereum_silver',
            'snapshot_voting'
        ) }}
) 
SELECT 
    id, 
    v.proposal_id, 
    voter, 
    vote_option, 
    voting_power, 
    vote_timestamp, 
    choices, 
    proposal_author, 
    proposal_title, 
    proposal_text, 
    space_id, 
    n.network, 
    delay, 
    quorum, 
    voting_period, 
    voting_type,
    proposal_start_time, 
    proposal_end_time,
    v._inserted_timestamp
FROM votes v
INNER JOIN proposals p
    ON v.proposal_id = p.proposal_id
LEFT JOIN networks n 
    ON p.network = n.chain_id
LEFT JOIN voting_strategy s
    ON p.space_id = s.name

