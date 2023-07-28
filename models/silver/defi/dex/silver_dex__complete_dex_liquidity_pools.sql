{{ config(
  materialized = 'incremental',
  unique_key = "_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH contracts AS (

  SELECT
    address,
    symbol,
    decimals,
    _inserted_timestamp
  FROM
    {{ ref('silver__contracts') }}
),

balancer AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    pool_name,
    'balancer' AS platform,
    _log_id AS _id,
    _inserted_timestamp,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7
FROM
    {{ ref('silver_dex__balancer_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

curve AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    deployer_address AS contract_address,
    pool_address,
    pool_name,
    'curve' AS platform,
    _call_id AS _id,
    _inserted_timestamp,
    MAX(CASE WHEN token_num = 1 THEN token_address END) AS token0,
    MAX(CASE WHEN token_num = 2 THEN token_address END) AS token1,
    MAX(CASE WHEN token_num = 3 THEN token_address END) AS token2,
    MAX(CASE WHEN token_num = 4 THEN token_address END) AS token3,
    MAX(CASE WHEN token_num = 5 THEN token_address END) AS token4,
    MAX(CASE WHEN token_num = 6 THEN token_address END) AS token5,
    MAX(CASE WHEN token_num = 7 THEN token_address END) AS token6,
    MAX(CASE WHEN token_num = 8 THEN token_address END) AS token7
FROM
    {{ ref('silver_dex__curve_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
GROUP BY all
),

dodo_v1 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    base_token AS token0,
    quote_token AS token1,
    'dodo-v1' AS platform,
    _id,
    _inserted_timestamp
FROM 
    {{ ref('silver_dex__dodo_v1_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

dodo_v2 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    base_token AS token0,
    quote_token AS token1,
    'dodo-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM 
    {{ ref('silver_dex__dodo_v2_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

frax AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    factory_address AS contract_address,
    pool_address,
    token0,
    token1,
    'fraxswap' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__fraxswap_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

kyberswap_v1_dynamic AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    token0,
    token1,
    'kyberswap-v1' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

kyberswap_v1_static AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    token0,
    token1,
    'kyberswap-v1' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__kyberswap_v1_static_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

kyberswap_v2_elastic AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    token0,
    token1,
    'kyberswap-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

maverick AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    tokenA AS token0,
    tokenB AS token1,
    'maverick' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__maverick_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

shibaswap AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    token0,
    token1,
    'shibaswap' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__shibaswap_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

pancakeswap_v2_amm AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    token0,
    token1,
    'pancakeswap-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pancakeswap_v2_amm_pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

pancakeswap_v3 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    'pancakeswap-v3' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pancakeswap_v3_pools') }} 
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

uni_sushi_v2_v3 AS (

SELECT
    creation_block AS block_number,
    creation_time AS block_timestamp,
    creation_tx AS tx_hash,
    factory_address AS contract_address,
    pool_address,
    pool_name,
    fee,
    tickSpacing AS tick_spacing,
    token0,
    token1,
    platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pools') }}
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

all_pools_standard AS (
    SELECT *
    FROM dodo_v1
    UNION ALL
    SELECT *
    FROM dodo_v2
    UNION ALL
    SELECT *
    FROM frax
    UNION ALL
    SELECT *
    FROM kyberswap_v1_dynamic
    UNION ALL
    SELECT *
    FROM kyberswap_v1_static
    UNION ALL
    SELECT *
    FROM kyberswap_v2_elastic
    UNION ALL
    SELECT *
    FROM maverick
    UNION ALL
    SELECT *
    FROM shibaswap
    UNION ALL
    SELECT *
    FROM pancakeswap_v2_amm
),

all_pools_v3 AS (
    SELECT *
    FROM uni_sushi_v2_v3
    UNION ALL
    SELECT *
    FROM pancakeswap_v3
),

all_pools_other AS (
    SELECT *
    FROM balancer
    UNION ALL
    SELECT *
    FROM curve
),

FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        CONCAT(  
            COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),
            '-',
            COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42)))
        ) AS pool_name,
        OBJECT_CONSTRUCT('token0',token0,'token1',token1) AS tokens,
        OBJECT_CONSTRUCT('token0',c0.symbol,'token1',c1.symbol) AS symbols,
        OBJECT_CONSTRUCT('token0',c0.decimals,'token1',c1.decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_standard p 
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        CASE
            WHEN pool_name IS NULL AND platform = 'sushiswap' 
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' SLP')
            WHEN pool_name IS NULL AND platform = 'uniswap-v2' 
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' UNI-V2 LP')
            WHEN pool_name IS NULL AND platform = 'uniswap-v3' 
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' ',COALESCE(fee,0),' ',COALESCE(tick_spacing,0),' UNI-V3 LP')
            WHEN pool_name IS NULL AND platform = 'pancakeswap-v3'
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' ',COALESCE(fee,0),' ',COALESCE(tick_spacing,0),' PCS-V3 LP')
            ELSE pool_name
        END AS pool_name,
        OBJECT_CONSTRUCT('token0',token0,'token1',token1) AS tokens,
        OBJECT_CONSTRUCT('token0',c0.symbol,'token1',c1.symbol) AS symbols,
        OBJECT_CONSTRUCT('token0',c0.decimals,'token1',c1.decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_v3 p 
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        CONCAT(
            COALESCE(c0.symbol, SUBSTRING(token0, 1, 5) || '...' || SUBSTRING(token0, 39, 42)),
            CASE WHEN token1 IS NOT NULL THEN '-' || COALESCE(c1.symbol, SUBSTRING(token1, 1, 5) || '...' || SUBSTRING(token1, 39, 42)) ELSE '' END,
            CASE WHEN token2 IS NOT NULL THEN '-' || COALESCE(c2.symbol, SUBSTRING(token2, 1, 5) || '...' || SUBSTRING(token2, 39, 42)) ELSE '' END,
            CASE WHEN token3 IS NOT NULL THEN '-' || COALESCE(c3.symbol, SUBSTRING(token3, 1, 5) || '...' || SUBSTRING(token3, 39, 42)) ELSE '' END,
            CASE WHEN token4 IS NOT NULL THEN '-' || COALESCE(c4.symbol, SUBSTRING(token4, 1, 5) || '...' || SUBSTRING(token4, 39, 42)) ELSE '' END,
            CASE WHEN token5 IS NOT NULL THEN '-' || COALESCE(c5.symbol, SUBSTRING(token5, 1, 5) || '...' || SUBSTRING(token5, 39, 42)) ELSE '' END,
            CASE WHEN token6 IS NOT NULL THEN '-' || COALESCE(c6.symbol, SUBSTRING(token6, 1, 5) || '...' || SUBSTRING(token6, 39, 42)) ELSE '' END,
            CASE WHEN token7 IS NOT NULL THEN '-' || COALESCE(c7.symbol, SUBSTRING(token7, 1, 5) || '...' || SUBSTRING(token7, 39, 42)) ELSE '' END
        ) AS pool_name,
        OBJECT_CONSTRUCT('token0', token0, 'token1', token1, 'token2', token2, 'token3', token3, 'token4', token4, 'token5', token5, 'token6', token6, 'token7', token7) AS tokens,
        OBJECT_CONSTRUCT('token0', c0.symbol, 'token1', c1.symbol, 'token2', c2.symbol, 'token3', c3.symbol, 'token4', c4.symbol, 'token5', c5.symbol, 'token6', c6.symbol, 'token7', c7.symbol) AS symbols,
        OBJECT_CONSTRUCT('token0', c0.decimals, 'token1', c1.decimals, 'token2', c2.decimals, 'token3', c3.decimals, 'token4', c4.decimals, 'token5', c5.decimals, 'token6', c6.decimals, 'token7', c7.decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_other p
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
    LEFT JOIN contracts c2
        ON c2.address = p.token2
    LEFT JOIN contracts c3
        ON c3.address = p.token3
    LEFT JOIN contracts c4
        ON c4.address = p.token4
    LEFT JOIN contracts c5
        ON c5.address = p.token5
    LEFT JOIN contracts c6
        ON c6.address = p.token6
    LEFT JOIN contracts c7
        ON c7.address = p.token7
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    platform,
    contract_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    _id,
    _inserted_timestamp
FROM FINAL 