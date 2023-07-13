{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS (

  SELECT
    address,
    symbol,
    NAME,
    decimals
  FROM
    {{ ref('core__dim_contracts') }}
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT address
      FROM
        contracts
    )

{% if is_incremental() %}
AND HOUR >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
coinbase AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  token_address,
  token_amount,
  token_amount_adj,
  p.price AS token_price_usd,
  (token_amount_adj * p.price) / w.price AS eth_amount_adj,
    eth_amount_adj * pow(10,18) AS eth_amount,
  w.price AS eth_price_usd,
  platform,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__coinbase_deposits')}} c
  LEFT JOIN prices w ON w.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND DATE_TRUNC('hour',block_timestamp) = w.hour
  LEFT JOIN prices p ON p.token_address = c.token_address
    AND DATE_TRUNC('hour',block_timestamp) = c.hour
),
binance AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__binance_deposits')}}
),
ankr AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__ankr_deposits')}}
),
cream AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__cream_deposits')}}
),
frax AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__fraxether_deposits')}}
),
hord AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__hord_deposits')}}
),
lido AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__lido_deposits')}}
),
nodedao AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__nodedao_deposits')}}
),
rocketpool AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__rocketpool_deposits')}}
),
sharedstake AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__sharedstake_deposits')}}
),
stafi AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__stafi_deposits')}}
),
stakehound AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__stakehound_deposits')}}
),
stakewise AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__stakewise_deposits')}}
),
swell AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__swell_deposits')}}
),
unieth AS (
SELECT 
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  contract_address,
  staker AS sender,
  staker AS recipient,
  amount AS eth_amount,
  amount_adj AS eth_amount_adj,
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__unieth_deposits')}}
),

--union all standard lsd CTEs here (excludes amount_usd)
all_lsd_standard AS (
  SELECT * FROM binance
  UNION ALL
  SELECT * FROM ankr
  UNION ALL
  SELECT * FROM cream
  UNION ALL
  SELECT * FROM frax
  UNION ALL
  SELECT * FROM hord
  UNION ALL
  SELECT * FROM lido
  UNION ALL
  SELECT * FROM nodedao
  UNION ALL
  SELECT * FROM rocketpool
  UNION ALL
  SELECT * FROM sharedstake
  UNION ALL
  SELECT * FROM stafi
  UNION ALL
  SELECT * FROM stakehound
  UNION ALL
  SELECT * FROM stakewise
  UNION ALL
  SELECT * FROM swell
  UNION ALL
  SELECT * FROM unieth
),
--union all non-standard lsd CTEs here (excludes amount_usd)
all_lsd_custom AS (
  SELECT 
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    staker AS sender,
    staker AS recipient,
    token_address,
    token_amount,
    token_amount_adj,
    _log_id,
    _inserted_timestamp
  FROM coinbase

),
--final unions standard and custom, includes prices
FINAL AS (
  
)
SELECT

FROM
  FINAL
