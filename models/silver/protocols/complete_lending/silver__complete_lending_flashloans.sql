{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH flashloans AS (

  SELECT
    TX_HASH,
    BLOCK_NUMBER,
    BLOCK_TIMESTAMP,
    EVENT_INDEX,
    AAVE_MARKET,
    AAVE_TOKEN,
    FLASHLOAN_AMOUNT,
    FLASHLOAN_AMOUNT_USD,
    PREMIUM_AMOUNT,
    PREMIUM_AMOUNT_USD,
    INITIATOR_ADDRESS,
    TARGET_ADDRESS,
    AAVE_VERSION,
    TOKEN_PRICE,
    SYMBOL,
    BLOCKCHAIN,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_ez_flashloans') }}
  
{% if is_incremental() %}
where _inserted_timestamp >= (
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
  flashloans
