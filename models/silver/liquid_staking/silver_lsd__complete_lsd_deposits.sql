{# {{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS (

  SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata
  FROM
    {{ ref('core__dim_contracts') }}
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT address
      FROM
        contracts
    )

{% if is_incremental() %}
AND HOUR >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
ankr AS ()
...


--union all standard lsd CTEs here (excludes amount_usd)
all_lsd_standard AS (
  
),
--union all non-standard lsd CTEs here (excludes amount_usd)
all_lsd_custom AS (
  
),
--final unions standard and custom, includes prices
FINAL AS (
  
)
SELECT

FROM
  FINAL #}
