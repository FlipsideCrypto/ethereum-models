{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

with repayments as (
    select
        TX_HASH,
        BLOCK_NUMBER,
        BLOCK_TIMESTAMP,
        EVENT_INDEX,
        AAVE_MARKET AS repay_token,
        AAVE_TOKEN as protocol_token,
        REPAYED_TOKENS as repayed_tokens,
        REPAYED_USD as repayed_usd,
        SYMBOL as repay_symbol,
        PAYER as payer_address,
        BORROWER as borrower_address,
        LENDING_POOL_CONTRACT,
        AAVE_VERSION as protocol,
        BLOCKCHAIN,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    from
        {{ref('silver__aave_ez_repayments')}}
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
        underlying_asset as repay_token,
        asset AS protocol_token,
        SUPPLY_TOKENS as repayed_tokens,
        SUPPLY_USD as repayed_usd,
        compound_market_symbol as repay_symbol,
        repay_address as payer_address,
        Borrow_address as borrower_address,
        NULL AS LENDING_POOL_CONTRACT,
        COMPOUND_VERSION AS protocol,
        BLOCKCHAIN,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ref('silver__compv3_ez_repayments')}}
    UNION ALL
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
    SELECT
        TX_HASH,
        BLOCK_NUMBER,
        BLOCK_TIMESTAMP,
        EVENT_INDEX,
        REPAY_CONTRACT_ADDRESS as repay_token,
        Ctoken as protocol_token,
        REPAYED_AMOUNT as repay_tokens,
        REPAYED_AMOUNT_USD as repay_usd,
        REPAY_CONTRACT_SYMBOL as repay_symbol,
        PAYER as payer_address,
        Borrower as borrow_address,
        NULL AS lending_pool_contract,
        'Comp V3' as protocol,
        'ethereum' as blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    from
        {{ref('compound__ez_repayments')}}
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
select
    *
from
    repayments