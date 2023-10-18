{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curation']
) }}

WITH log_join AS (

  SELECT
    tx_hash,
    block_timestamp,
    block_number,
    event_index,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS caller,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS owner,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS deposit_amount,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER / pow(
      10,
      decimals
    ) AS deposit_shares,
    deposit_amount / NULLIF(
      deposit_shares,
      0
    ) AS deposit_share_price,
    f.frax_market_address,
    f.frax_market_symbol,
    f.underlying_asset,
    f.underlying_symbol,
    f.underlying_decimals,
    l._log_id,
    l._inserted_timestamp
  FROM
    {{ ref('silver__fraxlend_asset_details') }}
    f
    LEFT JOIN {{ ref('silver__logs') }}
    l
    ON f.frax_market_address = l.contract_address
  WHERE
    topics [0] = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'

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
)
SELECT
  tx_hash,
  block_timestamp,
  block_number,
  event_index,
  caller,
  owner,
  deposit_amount,
  deposit_shares,
  deposit_share_price,
  frax_market_address,
  frax_market_symbol,
  underlying_asset AS deposit_asset,
  underlying_symbol,
  underlying_decimals,
  _log_id,
  _inserted_timestamp
FROM
  log_join qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
