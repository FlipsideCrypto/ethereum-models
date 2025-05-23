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
comp_repayments AS (
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
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS payer,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayed_amount_raw,
    'Compound V2' as compound_version,
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
      WHERE compound_version = 'Compound V2'
    )
    AND topics [0] :: STRING = '0x1a2a22cb034d26d1854bdc6666a5b91fe25efbbb5dcad3b0355478d6f5c362a1'

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
v3_repayments AS (

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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        contract_address AS ctoken,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS payer,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repayed_amount_raw,
        'Compound V3' AS compound_version,
        CONCAT(
          tx_hash,
          '-',
          event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        topics [0] = '0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e' --Supply
        AND contract_address IN (
            '0xa17581a9e3356d9a858b789d68b4d866e593ae94',
            '0xc3d688b66703497daa19211eedff47f25384cdc3'
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
    ctoken,
    C.ctoken_symbol,
    payer,
    repayed_amount_raw,
    C.underlying_asset_address AS repay_contract_address,
    C.underlying_symbol AS repay_contract_symbol,
    C.underlying_decimals,
    b.compound_version,
    b._log_id,
    b._inserted_timestamp
  FROM
    comp_repayments b
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
    ctoken,
    C.ctoken_symbol,
    payer,
    repayed_amount_raw,
    C.underlying_asset_address AS repay_contract_address,
    C.underlying_symbol AS repay_contract_symbol,
    C.underlying_decimals,
    b.compound_version,
    b._log_id,
    b._inserted_timestamp
  FROM
    v3_repayments b
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
    INNER JOIN asset_details
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
  ctoken,
  ctoken_symbol,
  payer,
  repay_contract_address,
  repay_contract_symbol,
  repayed_amount_raw AS repayed_amount_unadj,
  repayed_amount_raw / pow(
    10,
    underlying_decimals
  ) AS repayed_amount,
  ROUND(
    repayed_amount * p.token_price,
    2
  ) AS repayed_amount_usd,
  compound_version,
  _inserted_timestamp,
  _log_id,    
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS comp_repayments_id,
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

