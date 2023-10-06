{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime','reorg'],
    persist_docs ={ "relation": true,
    "columns": true }
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
comp_borrows AS (
  SELECT
    block_number,
    block_timestamp,
    event_index,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS borrower,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS loan_amount_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS accountBorrows,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER AS totalBorrows,
    contract_address AS ctoken,
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
    AND topics [0] :: STRING = '0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80'

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
    {{ ref('price__ez_hourly_token_prices') }}
    INNER JOIN asset_details
    ON token_address = underlying_asset_address
  WHERE
    HOUR :: DATE IN (
      SELECT
        block_timestamp :: DATE
      FROM
        comp_borrows
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
  CASE
    WHEN asset_details.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL
    ELSE asset_details.underlying_asset_address
  END AS borrows_contract_address,
  CASE
    WHEN asset_details.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH'
    ELSE asset_details.underlying_symbol
  END AS borrows_contract_symbol,
  ctoken,
  asset_details.ctoken_symbol AS ctoken_symbol,
  loan_amount_raw / pow(
    10,
    underlying_decimals
  ) AS loan_amount,
  ROUND((loan_amount_raw * p.token_price) / pow(10, underlying_decimals), 2) AS loan_amount_usd,
  _inserted_timestamp,
  _log_id
FROM
  comp_borrows
  LEFT JOIN prices p
  ON DATE_TRUNC(
    'hour',
    comp_borrows.block_timestamp
  ) = p.block_hour
  AND comp_borrows.ctoken = p.ctoken_address
  LEFT JOIN asset_details
  ON comp_borrows.ctoken = asset_details.ctoken_address
