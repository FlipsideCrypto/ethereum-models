{{ config(
  materialized = 'incremental',
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE'],
  meta={
      'database_tags':{
          'table': {
              'PROTOCOL': 'COMPOUND',
              'PURPOSE': 'DEFI'
          }
      }
  },
  tags = ['non_realtime']
) }}
-- pull all ctoken addresses and corresponding name
-- add the collateral liquidated here
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
comp_liquidations AS (
  SELECT
    block_number,
    block_timestamp,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrower,
    contract_address AS ctoken,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER AS seizeTokens_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS cTokenCollateral,
    tx_hash,
    event_index,
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
    AND topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'

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
        comp_liquidations
    )
  GROUP BY
    1,
    2,
    3
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  borrower,
  ctoken,
  asd1.ctoken_symbol AS ctoken_symbol,
  liquidator,
  seizeTokens_raw / pow(
    10,
    asd2.ctoken_decimals
  ) AS ctokens_seized,
  cTokenCollateral AS collateral_ctoken,
  asd2.ctoken_symbol AS collateral_symbol,
  repayAmount_raw / pow(
    10,
    asd1.underlying_decimals
  ) AS liquidation_amount,
  ROUND((repayAmount_raw * p.token_price) / pow(10, asd1.underlying_decimals), 2) AS liquidation_amount_usd,
  CASE
    WHEN asd1.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL
    ELSE asd1.underlying_asset_address
  END AS liquidation_contract_address,
  CASE
    WHEN asd1.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH'
    ELSE asd1.underlying_symbol
  END AS liquidation_contract_symbol,
  _inserted_timestamp,
  _log_id
FROM
  comp_liquidations
  LEFT JOIN prices p
  ON DATE_TRUNC(
    'hour',
    comp_liquidations.block_timestamp
  ) = p.block_hour
  AND comp_liquidations.ctoken = p.ctoken_address
  LEFT JOIN asset_details asd1
  ON comp_liquidations.ctoken = asd1.ctoken_address
  LEFT JOIN asset_details asd2
  ON comp_liquidations.cTokenCollateral = asd2.ctoken_address
