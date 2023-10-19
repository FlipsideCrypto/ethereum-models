{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg']
) }}

WITH ankr AS (

  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__ankr_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__binance_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    event_name,
    contract_address,
    sender,
    recipient,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__coinbase_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__cream_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__fraxether_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
guarded AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__guarded_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__hord_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__lido_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__nodedao_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__rocketpool_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__sharedstake_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
sharedstake_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__sharedstake_v2_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
stader AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stader_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stafi_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stakehound_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stakewise_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__swell_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
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
    event_name,
    contract_address,
    sender,
    recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__unieth_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
--union all standard lsd CTEs here
all_lsd_standard AS (
  SELECT * 
  FROM binance
  UNION ALL
  SELECT * 
  FROM ankr
  UNION ALL
  SELECT * 
  FROM cream
  UNION ALL
  SELECT * 
  FROM frax
  UNION ALL
  SELECT *
  FROM guarded
  UNION ALL
  SELECT * 
  FROM hord
  UNION ALL
  SELECT * 
  FROM lido
  UNION ALL
  SELECT * 
  FROM nodedao
  UNION ALL
  SELECT * 
  FROM rocketpool
  UNION ALL
  SELECT * 
  FROM sharedstake
  UNION ALL
  SELECT * 
  FROM sharedstake_v2
  UNION ALL
  SELECT * 
  FROM stader
  UNION ALL
  SELECT * 
  FROM stafi
  UNION ALL
  SELECT * 
  FROM stakehound
  UNION ALL
  SELECT * 
  FROM stakewise
  UNION ALL
  SELECT * 
  FROM swell
  UNION ALL
  SELECT * 
  FROM unieth
),
--union all non-standard lsd CTEs here
all_lsd_custom AS (
  SELECT * 
  FROM coinbase
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('price__ez_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT token_address
      FROM
        all_lsd_standard
      UNION
      SELECT
        DISTINCT token_address
      FROM
        all_lsd_custom
    )
    OR token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
),
FINAL AS (
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    token_amount,
    token_amount_adj,
    ROUND(token_amount_adj * p2.price,2) AS token_amount_usd,
    eth_amount_adj,
    eth_amount,
    ROUND(eth_amount_adj * p1.price,2) AS eth_amount_usd,
    s.token_address,
    token_symbol,
    platform,
    version,
    _log_id,
    _inserted_timestamp
  FROM
    all_lsd_standard s
    LEFT JOIN prices p1
    ON p1.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND DATE_TRUNC(
      'hour',
      s.block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON p2.token_address = s.token_address
    AND DATE_TRUNC(
      'hour',
      s.block_timestamp
    ) = p2.hour
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    token_amount,
    token_amount_adj,
    ROUND(token_amount_adj * p2.price,2) AS token_amount_usd,
    CASE 
      WHEN p2.price IS NULL THEN token_amount_adj
      ELSE (token_amount_adj * p2.price) / p1.price 
    END AS eth_amount_adj,
    eth_amount_adj * pow(
      10,
      18
    ) AS eth_amount,
    ROUND(eth_amount_adj * p1.price,2) AS eth_amount_usd,
    s.token_address,
    token_symbol,
    platform,
    version,
    _log_id,
    _inserted_timestamp
  FROM
    all_lsd_custom s
    LEFT JOIN prices p1
    ON p1.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND DATE_TRUNC(
      'hour',
      s.block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON p2.token_address = s.token_address
    AND DATE_TRUNC(
      'hour',
      s.block_timestamp
    ) = p2.hour
)
SELECT
  block_number,
  block_timestamp,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  tx_hash,
  event_index,
  event_name,
  contract_address,
  sender,
  recipient,
  eth_amount,
  eth_amount_adj,
  eth_amount_usd,
  token_amount,
  token_amount_adj,
  token_amount_usd,
  token_address,
  token_symbol,
  platform,
  version,
  _log_id,
  _inserted_timestamp
FROM
  FINAL
