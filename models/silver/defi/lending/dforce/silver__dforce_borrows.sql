{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}
-- pull all token addresses and corresponding name
WITH asset_details AS (

  SELECT
    token_address,
    token_symbol,
    token_name,
    token_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals
  FROM
    {{ ref('silver__dforce_asset_details') }}
),
dforce_borrows AS (
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
    contract_address AS token,
    'dForce' AS platform,
    _inserted_timestamp,
    _log_id
  FROM
    {{ ref('silver__logs') }}
  WHERE
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x2dd79f4fccfd18c360ce7f9132f3621bf05eee18f995224badb32d17f172df73'

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
dforce_combine AS (
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
    token,
    C.token_symbol,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM
    dforce_borrows b
    LEFT JOIN asset_details C
    ON b.token = C.token_address
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
  token as token_address,
  token_symbol,
  loan_amount_raw AS amount_unadj,
  loan_amount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  platform,
  _inserted_timestamp,
  _log_id
FROM
  dforce_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
