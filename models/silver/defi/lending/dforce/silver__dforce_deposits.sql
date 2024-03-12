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
dforce_deposits AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    contract_address AS token_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER AS minttokens_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS mintAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS supplier,
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
    AND topics [0] :: STRING = '0x2f00e3cdd69a77be7ed215ec7b2a36784dd158f921fca79ac29deffa353fe6ee'

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
    supplier,
    minttokens_raw,
    mintAmount_raw,
    C.underlying_asset_address AS supplied_contract_addr,
    C.underlying_symbol AS supplied_symbol,
    C.token_address,
    C.token_symbol,
    C.token_decimals,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM
    dforce_deposits b
    LEFT JOIN asset_details C
    ON b.token_address = C.token_address
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
  token_address,
  token_symbol,
  minttokens_raw / pow(
    10,
    token_decimals
  ) AS issued_tokens,
  mintAmount_raw AS amount_unadj,
  mintAmount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  supplied_contract_addr,
  supplied_symbol,
  supplier,
  platform,
  _inserted_timestamp,
  _log_id
FROM
  dforce_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
