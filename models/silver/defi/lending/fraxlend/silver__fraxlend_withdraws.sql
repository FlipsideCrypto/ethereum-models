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
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS caller,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS receiver,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 42)) AS owner,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS withdraw_amount_unadj,
    withdraw_amount_unadj / pow(
      10,
      f.underlying_decimals
    ) AS withdraw_amount,
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
    topics [0] = '0xbc290bb45104f73cf92115c9603987c3f8fd30c182a13603d8cffa49b5f59952'
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
  caller,
  receiver,
  owner,
  withdraw_amount_unadj,
  withdraw_amount,
  frax_market_address,
  frax_market_symbol,
  underlying_asset,
  underlying_symbol,
  underlying_decimals,
  _log_id,
  _inserted_timestamp
FROM
  log_join qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
