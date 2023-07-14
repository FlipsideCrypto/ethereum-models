{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE']
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__ankr_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    sender,
    recipient,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__coinbase_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__cream_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__fraxether_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),
lido AS (
  SELECT
    c.block_number,
    c.block_timestamp,
    c.origin_function_signature,
    c.origin_from_address,
    c.origin_to_address,
    c.tx_hash,
    c.event_index,
    c.contract_address,
    c.sender,
    c.recipient,
    c.eth_amount,
    c.eth_amount_adj,
    amount_of_shares AS token_amount,
    amount_of_shares_adj AS token_amount_adj,
    c.token_address,
    c.token_symbol,
    c.platform,
    c._log_id,
    c._inserted_timestamp
  FROM
    {{ ref('silver_lsd__lido_withdrawals_claimed') }} c
  LEFT JOIN {{ ref('silver_lsd__lido_withdrawal_requests') }} r
    USING(request_id)

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__nodedao_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__rocketpool_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__sharedstake_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__stafi_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
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
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_lsd__unieth_withdrawals') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),
--union all standard lsd CTEs here
all_lsd_standard AS (
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
  FROM stafi
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
    {{ ref('core__fact_hourly_token_prices') }}
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

{% if is_incremental() %}
AND HOUR >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
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
    contract_address,
    sender,
    recipient,
    token_amount,
    token_amount_adj,
    CASE 
      WHEN platform = coinbase AND block_timestamp < '2022-08-24 17:00:00' 
        THEN ROUND(token_amount_adj * p1.price,2)
      ELSE ROUND(token_amount_adj * p2.price,2)
    END AS token_amount_usd,
    CASE 
      WHEN platform = coinbase AND block_timestamp < '2022-08-24 17:00:00' 
        THEN (token_amount_adj * p1.price) / p1.price 
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
  _log_id,
  _inserted_timestamp
FROM
  FINAL
