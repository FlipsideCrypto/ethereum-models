{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

with deposits as (
    select
    TX_HASH,
    BLOCK_NUMBER,
    BLOCK_TIMESTAMP,
    EVENT_INDEX,
    AAVE_MARKET as deposit_asset,
    AAVE_TOKEN as market,
    ISSUED_TOKENS as deposit_tokens,
    SUPPLIED_USD as deposit_tokens_usd,
    DEPOSITOR_ADDRESS,
    LENDING_POOL_CONTRACT,
    NULL as ISSUED_CTOKENS,
    AAVE_VERSION as platform,
    SYMBOL,
    BLOCKCHAIN,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM 
    {{ref('silver__aave_ez_deposits')}}
WHERE platform <> 'Aave AMM'
{% if is_incremental() %}
AND
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
select
    TX_HASH,
    BLOCK_NUMBER,
    BLOCK_TIMESTAMP,
    EVENT_INDEX,
    deposit_asset,
    COMPOUND_MARKET as market,
    SUPPLY_TOKENS as deposit_tokens,
    SUPPLY_USD as deposit_tokens_usd,
    DEPOSITOR_ADDRESS,
    NULL as lending_pool_contract,
    NULL as ISSUED_CTOKENS,
    COMPOUND_VERSION as platform,
    SYMBOL,
    BLOCKCHAIN,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM 
    {{ref('silver__compv3_ez_deposits')}}
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
    TX_HASH,
    BLOCK_NUMBER,
    BLOCK_TIMESTAMP,
    EVENT_INDEX,
    SUPPLIED_CONTRACT_ADDR as deposit_asset,
    CTOKEN as market,
    SUPPLIED_BASE_ASSET as deposit_tokens,
    SUPPLIED_BASE_ASSET_USD as deposit_tokens_usd,
    SUPPLIER as DEPOSITOR_ADDRESS,
    NULL as lending_pool_contract,
    ISSUED_CTOKENS,
    'Compound V2' as platform,
    SUPPLIED_SYMBOL as symbol,
    'ethereum' as blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM 
    {{ref('compound__ez_deposits')}}
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
