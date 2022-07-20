{{
  config(
    materialized='incremental',
    sort='block_number',
    unique_key= "CONCAT_WS('-', tx_hash, voter)",
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_votes']
  )
}}




SELECT block_number,
       block_timestamp,
       contract_address AS governance_contract,
       event_inputs:id::NUMERIC AS proposal_id,
       event_inputs:support::STRING AS support,
       event_inputs:votingPower::NUMERIC AS voting_power,
       LOWER(event_inputs:voter::STRING) AS voter,
       tx_hash,
       'ethereum' AS blockchain
FROM  {{ref('core__fact_event_logs')}}
WHERE 
event_name = 'VoteEmitted' AND contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'