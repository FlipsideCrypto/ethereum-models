{{ config(
  materialized = 'incremental',
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['compound']
) }}
-- pull all ctoken addresses and corresponding name
WITH asset_details AS (

  SELECT
    ctoken_address,
    ctoken_symbol,
    ctoken_name,
    ctoken_decimals,
    underlying_asset_address,
    ctoken_metadata,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    underlying_contract_metadata
  FROM
    {{ ref('compound__ez_asset_details') }}
),
comp_repayments AS (
  SELECT
    DISTINCT block_number,
    block_timestamp,
    REGEXP_REPLACE(
      event_inputs :borrower,
      '\"',
      ''
    ) AS borrower,
    contract_address AS ctoken,
    REGEXP_REPLACE(
      event_inputs :payer,
      '\"',
      ''
    ) AS payer,
    event_inputs :repayAmount AS repayed_amount_raw,
    tx_hash,
    _inserted_timestamp,
    _log_id
  FROM
    {{ ref('silver__logs') }}
  WHERE
    contract_address IN (
      SELECT
        ctoken_address
      FROM
        asset_details
    )
    AND event_name = 'RepayBorrow'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
--pull hourly prices for each undelrying
prices AS (
  SELECT
    HOUR AS block_hour,
    token_address AS token_contract,
    ctoken_address,
    AVG(price) AS token_price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
    INNER JOIN asset_details
    ON token_address = underlying_asset_address
  WHERE
    HOUR :: DATE IN (
      SELECT
        block_timestamp :: DATE
      FROM
        comp_repayments
    )
  GROUP BY
    1,
    2,
    3
)
SELECT
  block_number,
  block_timestamp,
  borrower,
  ctoken,
  asset_details.ctoken_symbol AS ctoken_symbol,
  payer,
  CASE
    WHEN asset_details.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL
    ELSE asset_details.underlying_asset_address
  END AS repay_contract_address,
  CASE
    WHEN asset_details.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH'
    ELSE asset_details.underlying_symbol
  END AS repay_contract_symbol,
  repayed_amount_raw / pow(
    10,
    underlying_decimals
  ) AS repayed_amount,
  ROUND(
    repayed_amount * p.token_price,
    2
  ) AS repayed_amount_usd,
  tx_hash,
  _inserted_timestamp,
  _log_id
FROM
  comp_repayments
  LEFT JOIN prices p
  ON DATE_TRUNC(
    'hour',
    comp_repayments.block_timestamp
  ) = p.block_hour
  AND comp_repayments.ctoken = p.ctoken_address
  LEFT JOIN asset_details
  ON comp_repayments.ctoken = asset_details.ctoken_address
