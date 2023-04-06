{{ config(
  materialized = 'view',
  tags = ['snowflake', 'ethereum', 'aave', 'aave_proposals'],
  meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'AAVE',
  'PURPOSE': 'DEFI' }}}
) }}

SELECT
  p.block_number,
  p.start_voting_period,
  p.end_voting_period,
  b.block_timestamp,
  p.contract_address AS governance_contract,
  p.proposal_id,
  CASE
    WHEN C.current_block > p.end_voting_period
    AND e.status IS NULL
    AND q.status IS NULL THEN 'Failed'
    ELSE COALESCE(
      e.status,
      q.status,
      p.status
    )
  END AS status,
  p.targets,
  p.proposer,
  p.tx_hash AS proposal_tx,
  'ethereum' AS blockchain,
  q.tx_hash AS queue_tx,
  e.tx_hash AS execution_tx,
  svp.block_timestamp AS start_voting_period_timestamp,
  evp.block_timestamp AS end_voting_period_timestamp,
  q.block_number AS queue_block_number,
  qb.block_timestamp AS queue_timestamp,
  e.block_number AS execution_block_number,
  eb.block_timestamp AS execution_timestamp,
  v.total_voting_power,
  v.yay_voting_power,
  v.nay_voting_power
FROM
  {{ ref('silver__aave_created_proposals') }}
  p
  LEFT OUTER JOIN {{ ref('silver__aave_queued_proposals') }}
  q
  ON p.proposal_id = q.proposal_id
  LEFT OUTER JOIN {{ ref('silver__aave_executed_proposals') }}
  e
  ON p.proposal_id = e.proposal_id
  LEFT OUTER JOIN {{ ref('_block_ranges') }}
  b
  ON p.block_number = b.block_number
  LEFT OUTER JOIN (
    SELECT
      MAX(block_number) AS current_block
    FROM
      {{ ref('_block_ranges') }}
  ) C
  ON 1 = 1
  LEFT JOIN (
    SELECT
      proposal_id,
      SUM(voting_power / pow(10, 18)) AS total_voting_power,
      SUM(
        CASE
          WHEN support THEN voting_power
        END / pow(
          10,
          18
        )
      ) AS yay_voting_power,
      SUM(
        CASE
          WHEN NOT support THEN voting_power
        END / pow(
          10,
          18
        )
      ) AS nay_voting_power
    FROM
      {{ ref('aave__ez_votes') }}
    GROUP BY
      1
  ) v
  ON v.proposal_id = p.proposal_id
  LEFT JOIN {{ ref('_block_ranges') }}
  svp
  ON svp.block_number = p.start_voting_period
  LEFT JOIN {{ ref('_block_ranges') }}
  evp
  ON evp.block_number = p.end_voting_period
  LEFT JOIN {{ ref('_block_ranges') }}
  eb
  ON eb.block_number = e.block_number
  LEFT JOIN {{ ref('_block_ranges') }}
  qb
  ON qb.block_number = q.block_number
