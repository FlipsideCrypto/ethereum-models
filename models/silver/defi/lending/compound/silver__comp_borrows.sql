{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['silver','defi','lending','curated']
) }}
-- pull all ctoken addresses and corresponding name
WITH asset_details AS (

    SELECT
        ctoken_address,
        ctoken_symbol,
        ctoken_name,
        ctoken_decimals,
        underlying_asset_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals,
        compound_version
    FROM
        {{ ref('silver__comp_asset_details') }}

),
compv2_borrows AS (
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
    'Compound V2' AS compound_version,
    modified_timestamp AS _inserted_timestamp,
    CONCAT(
      tx_hash :: STRING,
      '-',
      event_index :: STRING
    ) AS _log_id
  FROM
    {{ ref('core__fact_event_logs') }}
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
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
compv3_borrows AS (
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
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS src_address,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS loan_amount_raw,
    origin_from_address AS borrower,
    contract_address AS ctoken,
    'Compound V3' AS compound_version,
    CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
    l.modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__fact_event_logs') }}
    l
  WHERE
    topics [0] = '0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb' --withdrawl
    AND contract_address IN (
      SELECT
        ctoken_address
      FROM
        asset_details
      WHERE
        compound_version = 'Compound V3'
    )

{% if is_incremental() %}
AND l.modified_timestamp >= (
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
    borrower,
    loan_amount_raw,
    C.underlying_asset_address AS borrows_contract_address,
    C.underlying_symbol AS borrows_contract_symbol,
    ctoken,
    C.ctoken_symbol,
    C.underlying_decimals,
    b.compound_version,
    b._log_id,
    b._inserted_timestamp
  FROM
    compv2_borrows b
    LEFT JOIN {{ ref('silver__comp_asset_details') }} C
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
    borrower,
    loan_amount_raw,
    C.underlying_asset_address AS borrows_contract_address,
    C.underlying_symbol AS borrows_contract_symbol,
    ctoken,
    C.ctoken_symbol,
    C.underlying_decimals,
    b.compound_version,
    b._log_id,
    b._inserted_timestamp
  FROM
    compv3_borrows b
    LEFT JOIN {{ ref('silver__comp_asset_details') }} C
    ON b.ctoken = C.ctoken_address
),
--pull hourly prices for each undelrying
prices AS (
  SELECT
    HOUR AS block_hour,
    token_address AS token_contract,
    ctoken_address,
    AVG(price) AS token_price
  FROM
    {{ ref('price__ez_prices_hourly') }}
    INNER JOIN {{ ref('silver__comp_asset_details') }}
    ON token_address = underlying_asset_address
  WHERE
    HOUR :: DATE IN (
      SELECT
        block_timestamp :: DATE
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
  borrower,
  borrows_contract_address,
  borrows_contract_symbol,
  ctoken,
  ctoken_symbol,
  loan_amount_raw,
  loan_amount_raw / pow(
    10,
    underlying_decimals
  ) AS loan_amount,
  ROUND((loan_amount_raw * p.token_price) / pow(10, underlying_decimals), 2) AS loan_amount_usd,
  compound_version,
  _inserted_timestamp,
  _log_id,    
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS comp_borrows_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
  comp_combine
  LEFT JOIN prices p
  ON DATE_TRUNC(
    'hour',
    comp_combine.block_timestamp
  ) = p.block_hour
  AND comp_combine.ctoken = p.ctoken_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
