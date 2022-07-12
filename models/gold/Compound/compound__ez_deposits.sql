{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

-- pull all ctoken addresses and corresponding name
WITH asset_details AS (

  SELECT
    ctoken_address,
    ctoken_symbol,
    ctoken_name,
    ctoken_decimals,
    underlying_asset_address,
    ctoken_metadata,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    underlying_contract_metadata
  FROM
    {{ ref('compound__ez_asset_details') }}
),

-- look up underlying token
underlying AS (
  SELECT DISTINCT 
    contract_address as address, 
    LOWER(value_string) as token_contract
  FROM {{source('flipside_silver_ethereum','reads')}}
  WHERE 
    contract_address IN (SELECT ctoken_address FROM asset_details)
    AND function_name = 'underlying'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
   
   UNION
   
   -- this grabs weth for the cETH contract
  SELECT DISTINCT
    contract_address AS address, 
    LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token_contract
  FROM {{source('flipside_silver_ethereum','reads')}}
  WHERE 
    contract_address = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}   
),

--pull hourly prices for each undelrying
prices AS (
  SELECT
    HOUR AS block_hour,
    token_address AS token_contract, -- this is the undelrying asset
    ctoken_address as address, -- this is the ctoken
    AVG(price) AS token_price
  FROM {{ ref('core__fact_hourly_token_prices') }}

INNER JOIN asset_details
  ON token_address = underlying_asset_address

WHERE     
      {% if is_incremental() %}
          hour >= getdate() - interval '2 days'
      {% else %}
          hour >= getdate() - interval '9 months'
      {% endif %}  
  
  GROUP BY
    1,
    2,
    3
)

SELECT 
  DISTINCT block_number,
    block_timestamp,
    ee.contract_address AS ctoken, 
    ctoken_symbol,
    event_inputs:mintTokens/pow(10,ctoken_decimals) AS issued_ctokens,
    event_inputs:mintAmount/pow(10,underlying_decimals) AS supplied_base_asset, 
    event_inputs:mintAmount*p.token_price/pow(10,underlying_decimals) AS supplied_base_asset_usd,
    CASE WHEN p.token_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL ELSE p.token_contract END AS supplied_contract_addr,
    CASE WHEN p.token_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH' ELSE underlying_symbol END AS supplied_symbol,
    REGEXP_REPLACE(event_inputs:minter,'\"','') AS supplier,
    tx_hash
FROM {{ ref('core__fact_event_logs') }} ee 

LEFT JOIN prices p
  ON date_trunc('hour',ee.block_timestamp) = p.block_hour 
  AND ee.contract_address = p.address

LEFT OUTER JOIN asset_details ad
  ON ee.contract_address = ad.ctoken_address

WHERE
    {% if is_incremental() %}
    block_timestamp >= getdate() - interval '2 days'
    {% else %}
    block_timestamp >= getdate() - interval '9 months'
    {% endif %}   
  AND ee.contract_address IN (select ctoken_address from asset_details)
  AND ee.event_name = 'Mint'
  AND ctoken_decimals IS NOT NULL
  AND ctoken_address IS NOT NULL