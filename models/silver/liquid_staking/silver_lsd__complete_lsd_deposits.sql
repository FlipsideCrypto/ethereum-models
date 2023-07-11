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
  {# amount AS eth_amount,
  amount_adj AS eth_amount_adj, #}
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__binance_deposits')}}
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
  {# amount AS eth_amount,
  amount_adj AS eth_amount_adj, #}
  eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj,
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__coinbase_deposits')}}
),
etherfi AS (
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
  {# eth_amount AS token_amount,
  eth_amount_adj AS token_amount_adj, #}
  _log_id,
  _inserted_timestamp
FROM {{ref('silver_lsd__etherfi_deposits')}}
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
...


--union all standard lsd CTEs here (excludes amount_usd)
all_lsd_standard AS (
  
),
--union all non-standard lsd CTEs here (excludes amount_usd)
all_lsd_custom AS (
  
),
--final unions standard and custom, includes prices
FINAL AS (
  
)
SELECT

FROM
  FINAL
