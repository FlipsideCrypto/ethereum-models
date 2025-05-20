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
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS borrower,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS receiver,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS borrow_amount_unadj,
    borrow_amount_unadj / pow(
      10,
      decimals
    ) AS borrow_amount,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS shares_added,
    borrow_amount / NULLIF(
      shares_added,
      0
    ) AS borrow_share_price,
    f.frax_market_address,
    f.frax_market_symbol,
    f.underlying_asset,
    f.underlying_symbol,
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
    topics [0] = '0x01348584ec81ac7acd52b7d66d9ade986dd909f3d513881c190fc31c90527efe'
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
  borrower,
  receiver,
  borrow_amount_unadj,
  borrow_amount,
  shares_added,
  borrow_share_price,
  frax_market_address,
  frax_market_symbol,
  lower('0x853d955aCEf822Db058eb8505911ED77F175b99e') AS borrow_asset,
  'FRAX' AS borrow_symbol,
  underlying_asset AS collateral_asset,
  underlying_symbol,
  _log_id,
  _inserted_timestamp
FROM
  log_join qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
