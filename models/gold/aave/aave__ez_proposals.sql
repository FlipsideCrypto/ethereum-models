{{
  config(
    materialized='incremental',
    sort='block_number',
    unique_key= "CONCAT_WS('-', proposal_tx, proposal_id)",
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_proposals']
  )
}}


WITH 
  p AS (
  SELECT 
      block_number,
      block_timestamp,
      contract_address AS governance_contract,
      event_inputs:id::STRING AS proposal_id,
      'Created' AS status,
      event_inputs:targets AS targets,
      LOWER(event_inputs:creator::STRING) AS proposer,
      tx_hash,
      event_inputs:endBlock::INTEGER AS end_voting_period,
      event_inputs:startBlock::INTEGER AS start_voting_period,
    CURRENT_DATE AS now
  FROM {{ref('core__fact_event_logs')}}
  WHERE 
  event_name = 'ProposalCreated' AND contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'

), q AS (
    SELECT DISTINCT
        event_inputs:id::STRING AS id,
        'Queued' AS status
    FROM {{ref('core__fact_event_logs')}}
    WHERE 
    event_name = 'ProposalQueued' AND contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'
), e AS (
    
    SELECT DISTINCT
      COALESCE(event_inputs:id::STRING,event_inputs:proposalId::STRING) AS id,
      'Executed' AS status
    FROM {{ref('core__fact_event_logs')}}
    WHERE 
    event_name = 'ProposalExecuted' AND contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'


), c AS (
  SELECT 
  CURRENT_DATE AS now,
  MAX(block_number) AS current_block
  FROM {{ref('core__fact_event_logs')}} 
  WHERE block_timestamp >= CURRENT_DATE - 3
)


SELECT 
    p.block_number,
    p.start_voting_period,
    p.end_voting_period,
    p.block_timestamp,
    p.governance_contract,
    p.proposal_id,
    CASE 
        WHEN c.current_block > p.end_voting_period AND e.status IS NULL AND q.status IS NULL THEN 'Failed'
        ELSE COALESCE(e.status,q.status,p.status) END 
        AS status,
    p.targets,
    p.proposer,
    p.tx_hash AS proposal_tx,
    'ethereum' AS blockchain  
 FROM 
 p
 LEFT OUTER JOIN
 q
 ON p.proposal_id = q.id
 LEFT OUTER JOIN
 e
 ON p.proposal_id = e.id
 LEFT OUTER JOIN
 c
 ON p.now = c.now
 ORDER BY block_timestamp DESC