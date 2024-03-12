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
dforce_repayments AS (
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
    contract_address AS token,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS payer,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayed_amount_raw,
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
    AND topics [0] :: STRING = '0x6fadbf7329d21f278e724fa0d4511001a158f2a97ee35c5bc4cf8b64417399ef'

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
    token,
    C.token_symbol,
    payer,
    repayed_amount_raw,
    C.underlying_asset_address AS repay_contract_address,
    C.underlying_symbol AS repay_contract_symbol,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM
    dforce_repayments b
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
  token,
  token_symbol,
  payer,
  repay_contract_address,
  repay_contract_symbol,
  repayed_amount_raw AS amount_unadj,
  repayed_amount_raw / pow(
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
