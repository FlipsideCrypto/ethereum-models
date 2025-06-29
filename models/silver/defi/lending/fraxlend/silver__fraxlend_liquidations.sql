{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['silver','defi','lending','curated']
) }}

WITH log_join AS (

  SELECT
    tx_hash,
    block_timestamp,
    block_number,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    origin_from_address AS liquidator,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS borrower,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS collateral_for_liquidator_unadj,
    collateral_for_liquidator_unadj / pow(
      10,
      f.underlying_decimals
    ) AS collateral_for_liquidator,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS shares_to_liquidate,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS liquidator_repay_amount,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS shares_to_adjust,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS amount_to_adjust,
    liquidator_repay_amount / NULLIF(
      shares_to_liquidate,
      0
    ) AS liquidator_share_price,
    f.frax_market_address,
    f.frax_market_symbol,
    f.underlying_asset,
    f.underlying_symbol,
    f.underlying_decimals,
    CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
    l.modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver__fraxlend_asset_details') }}
    f
    LEFT JOIN {{ ref('core__fact_event_logs') }}
    l
    ON f.frax_market_address = l.contract_address
  WHERE
    topics [0] = '0x35f432a64bd3767447a456650432406c6cacb885819947a202216eeea6820ecf'
    AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) - INTERVAL '12 hours'
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  liquidator,
  borrower,
  collateral_for_liquidator_unadj,
  collateral_for_liquidator,
  shares_to_liquidate,
  liquidator_repay_amount,
  shares_to_adjust,
  amount_to_adjust,
  liquidator_share_price,
  LOWER('0x853d955aCEf822Db058eb8505911ED77F175b99e') AS debt_asset,
  frax_market_address,
  frax_market_symbol,
  underlying_asset,
  underlying_symbol,
  underlying_decimals,
  _log_id,
  _inserted_timestamp
FROM
  log_join l qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
