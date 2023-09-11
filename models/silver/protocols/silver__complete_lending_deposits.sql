{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH deposits AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    aave_market AS deposit_asset,
    aave_token AS market,
    issued_tokens AS deposit_tokens,
    supplied_usd AS deposit_tokens_usd,
    depositor_address,
    lending_pool_contract,
    NULL AS issued_ctokens,
    aave_version AS platform,
    symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_ez_deposits') }}
  WHERE
    platform <> 'Aave AMM'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 1
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  deposit_asset,
  compound_market AS market,
  supply_tokens AS deposit_tokens,
  supply_usd AS deposit_tokens_usd,
  depositor_address,
  NULL AS lending_pool_contract,
  NULL AS issued_ctokens,
  compound_version AS platform,
  symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__compv3_ez_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  supplied_contract_addr AS deposit_asset,
  ctoken AS market,
  supplied_base_asset AS deposit_tokens,
  supplied_base_asset_usd AS deposit_tokens_usd,
  supplier AS depositor_address,
  NULL AS lending_pool_contract,
  issued_ctokens,
  'Compound V2' AS platform,
  supplied_symbol AS symbol,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('compound__ez_deposits') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  *
FROM
  deposits
