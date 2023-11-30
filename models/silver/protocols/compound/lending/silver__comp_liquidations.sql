{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
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
    underlying_contract_metadata,
    compound_version
  FROM
    {{ ref('silver__comp_asset_details') }}
),
compv2_liquidations AS (
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
    contract_address AS ctoken,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER AS seizeTokens_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS cTokenCollateral,
    'Compound V2' AS compound_version,
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
      WHERE
        compound_version = 'Compound V2'
    )
    AND topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) - INTERVAL '36 hours'
  FROM
    {{ this }}
)
{% endif %}
),
compv3_liquidations AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
    contract_address AS ctoken,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS liquidator,
    NULL AS seizeTokens_raw,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS repayAmount_raw,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS liquidated_amount_usd,
    'Compound V3' AS compound_version,
    C.name,
    C.symbol,
    C.decimals,
    _log_id,
    l._inserted_timestamp
  FROM
    {{ ref('silver__logs') }}
    l
  LEFT JOIN 
    {{ ref('silver__contracts') }} C
  ON 
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) = C.address
  WHERE
    topics [0] = '0x9850ab1af75177e4a9201c65a2cf7976d5d28e40ef63494b44366f86b2f9412e' --AbsorbCollateral
    AND contract_address IN (
      SELECT
        ctoken_address
      FROM
        asset_details
      WHERE
        compound_version = 'Compound V3'
    )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) - INTERVAL '12 hours'
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
        compv2_liquidations
    )
  GROUP BY
    1,
    2,
    3
),
liquidation_union as (
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
    ctoken,
    asd1.ctoken_symbol AS ctoken_symbol,
    liquidator,
    seizeTokens_raw / pow(
      10,
      asd2.ctoken_decimals
    ) AS ctokens_seized,
    cTokenCollateral AS collateral_ctoken,
    asd2.ctoken_symbol AS collateral_ctoken_symbol,
    asd2.underlying_asset_address AS collateral_token,
    asd2.underlying_symbol AS collateral_symbol,
    repayAmount_raw AS liquidation_amount_unadj,
    repayAmount_raw / pow(
      10,
      asd1.underlying_decimals
    ) AS liquidation_amount,
    ROUND((repayAmount_raw * p.token_price) / pow(10, asd1.underlying_decimals), 2) AS liquidation_amount_usd,
    asd1.underlying_asset_address AS liquidation_contract_address,
    asd1.underlying_symbol AS liquidation_contract_symbol,
    l.compound_version,
    l._inserted_timestamp,
    l._log_id
  FROM
    compv2_liquidations l
    LEFT JOIN prices p
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.block_hour
    AND l.ctoken = p.ctoken_address
    LEFT JOIN asset_details asd1
    ON l.ctoken = asd1.ctoken_address
    LEFT JOIN asset_details asd2
    ON l.cTokenCollateral = asd2.ctoken_address
  UNION ALL
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
    ctoken,
    A.ctoken_symbol,
    liquidator,
    NULL AS ctokens_seized,
    NULL AS collateral_ctoken,
    NULL AS collateral_ctoken_symbol,
    A.underlying_asset_address AS collateral_token,
    A.underlying_symbol AS collateral_symbol,
    repayAmount_raw AS liquidation_amount_unadj,
    repayAmount_raw / pow(
      10,
      l.decimals
    ) AS liquidation_amount,
    liquidated_amount_usd / pow(
      10,
      8
    ) AS liquidation_amount_usd,
    asset AS liquidation_contract_address,
    c.symbol AS liquidation_contract_symbol,
    l.compound_version,
    l._inserted_timestamp,
    l._log_id
  FROM
    compv3_liquidations l
    LEFT JOIN {{ ref('silver__comp_asset_details') }} A
    ON l.ctoken = A.ctoken_address
    LEFT JOIN {{ ref('silver__contracts') }} c
    ON l.asset = c.address 
)
SELECT
  *,    
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS comp_liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
  liquidation_union qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

