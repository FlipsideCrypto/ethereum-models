{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curation']
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
    underlying_contract_metadata,
    compound_version
  FROM
    {{ ref('silver__comp_asset_details') }}
),
compv2_deposits AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    contract_address AS ctoken,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS mintTokens_raw,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS mintAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS supplier,
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
      where compound_version = 'Compound V2'
    )
    AND topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'

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
compv3_deposits AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS ctoken,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
        NULL AS mintTokens_raw,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS mintAmount_raw,
        origin_from_address AS supplier,
        'Compound V3' AS compound_version,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
      contract_address IN (
        SELECT
          ctoken_address
        FROM
          asset_details
        where compound_version = 'Compound V3'
      )
    AND
        topics [0] = '0xfa56f7b24f17183d81894d3ac2ee654e3c26388d17a28dbd9549b8114304e1f4' --SupplyCollateral

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
comp_combine AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    supplier,
    mintTokens_raw,
    mintAmount_raw,
    C.underlying_asset_address AS supplied_contract_addr,
    C.underlying_symbol AS supplied_symbol,
    ctoken,
    C.ctoken_symbol,
    c.ctoken_decimals,
    C.underlying_decimals,
    b.compound_version,
    b._log_id,
    b._inserted_timestamp
  FROM
    compv2_deposits b
    LEFT JOIN asset_details C
    ON b.ctoken = C.ctoken_address
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
    supplier,
    mintTokens_raw,
    mintAmount_raw,
    b.asset AS supplied_contract_addr,
    c.symbol AS supplied_symbol,
    ctoken,
    a.ctoken_symbol,
    a.ctoken_decimals,
    c.decimals as underlying_decimals,
    b.compound_version,
    b._log_id,
    b._inserted_timestamp
  FROM
    compv3_deposits b
  LEFT JOIN {{ ref('silver__contracts') }} C
  ON b.asset = C.address
  LEFT JOIN asset_details a
  ON b.ctoken = a.ctoken_address
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
        LEFT JOIN asset_details
        ON token_address = underlying_asset_address
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
              comp_combine
        )
        AND token_address in (
            SELECT
                supplied_contract_addr
            FROM
              comp_combine
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
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  ctoken,
  ctoken_symbol,
  mintTokens_raw / pow(
    10,
    ctoken_decimals
  ) AS issued_ctokens,
  mintAmount_raw / pow(
    10,
    underlying_decimals
  ) AS supplied_base_asset,
  ROUND((mintAmount_raw * p.token_price) / pow(10, underlying_decimals), 2) AS supplied_base_asset_usd,
  supplied_contract_addr,
  supplied_symbol,
  supplier,
  compound_version,
  _inserted_timestamp,
  _log_id
FROM
  comp_combine
  LEFT JOIN prices p
  ON DATE_TRUNC(
    'hour',
    comp_combine.block_timestamp
  ) = p.block_hour
  AND comp_combine.supplied_contract_addr = p.token_contract qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

