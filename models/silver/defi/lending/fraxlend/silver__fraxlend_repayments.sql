{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg']
) }}

WITH log_join AS (

  SELECT
    tx_hash,
    block_timestamp,
    block_number,
    event_index,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS payer,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS borrower,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS repay_amount,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS repay_shares,
    repay_amount / NULLIF(
      repay_shares,
      0
    ) AS repay_share_price,
    f.frax_market_address,
    f.frax_market_symbol,
    f.underlying_asset,
    l._log_id,
    l._inserted_timestamp
  FROM
    {{ ref('silver__fraxlend_asset_details') }}
    f
    LEFT JOIN {{ ref('silver__logs') }}
    l
    ON f.frax_market_address = l.contract_address
  WHERE
    topics [0] = '0x9dc1449a0ff0c152e18e8289d865b47acc6e1b76b1ecb239c13d6ee22a9206a7'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) - INTERVAL '36 hours'
  FROM
    {{ this }}
)
{% endif %}
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
        DISTINCT underlying_asset
      FROM
        {{ ref('silver__fraxlend_asset_details') }}
    )

{% if is_incremental() %}
AND HOUR >= (
  SELECT
    MAX(_inserted_timestamp) - INTERVAL '36 hours'
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  tx_hash,
  block_timestamp,
  block_number,
  event_index,
  payer,
  borrower,
  repay_amount,
  ROUND(
    repay_amount * p.price,
    2
  ) AS repay_amount_usd,
  repay_shares,
  repay_share_price,
  frax_market_address,
  frax_market_symbol,
  underlying_asset,
  _log_id,
  _inserted_timestamp
FROM
  log_join l
  LEFT JOIN prices p
  ON underlying_asset = p.token_address
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p.hour
