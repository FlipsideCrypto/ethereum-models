{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}
-- pull all itoken addresses and corresponding name
-- add the collateral liquidated here
WITH asset_details AS (

  SELECT
    itoken_address,
    itoken_symbol,
    itoken_name,
    itoken_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals
  FROM
    {{ ref('silver__strike_asset_details') }}
),
strike_liquidations AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrower,
    contract_address AS itoken,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER AS seizeTokens_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS itokenCollateral,
    'Strike' AS platform,
    _inserted_timestamp,
    _log_id
  FROM
    {{ ref('silver__logs') }}
  WHERE
    contract_address IN (
      SELECT
        itoken_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) - INTERVAL '12 hours'
  FROM
    {{ this }}
)
{% endif %}
),
liquidation_union AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    itoken,
    asd1.itoken_symbol AS itoken_symbol,
    liquidator,
    seizeTokens_raw / pow(
      10,
      asd2.itoken_decimals
    ) AS itokens_seized,
    itokenCollateral AS collateral_itoken,
    asd2.itoken_symbol AS collateral_itoken_symbol,
    asd2.underlying_asset_address AS collateral_token,
    asd2.underlying_symbol AS collateral_symbol,
    repayAmount_raw AS amount_unadj,
    repayAmount_raw / pow(
      10,
      asd1.underlying_decimals
    ) AS liquidation_amount,
    asd1.underlying_decimals,
    asd1.underlying_asset_address AS liquidation_contract_address,
    asd1.underlying_symbol AS liquidation_contract_symbol,
    l.platform,
    l._inserted_timestamp,
    l._log_id
  FROM
    strike_liquidations l
    LEFT JOIN asset_details asd1
    ON l.itoken = asd1.itoken_address
    LEFT JOIN asset_details asd2
    ON l.itokenCollateral = asd2.itoken_address
)
SELECT
  *
FROM
  liquidation_union qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
