{{ config(
  materialized = 'incremental',
  unique_key = "_id",
  cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS ( --join core dim_contracts in gold view instead if possible

  SELECT
    address,
    symbol,
    NAME,
    decimals,
    _inserted_timestamp
  FROM
    {{ ref('silver__contracts') }}
{# {% if is_incremental() %}
WHERE address NOT IN (
    SELECT 
        DISTINCT token_address AS --flatten tokens into one column
    FROM {{ this }}
    )
{% endif %} #}
),

{# balancer AS ( --custom

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    pool_name,
    NULL AS tokens,
    'balancer' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__balancer_pools') }}
),

curve AS ( --custom

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    deployer_address AS contract_address,
    pool_address,
    pool_name,
    ARRAY_AGG(token_address) AS tokens,
    'curve' AS platform,
    _call_id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__curve_pools') }}
),

hashflow AS ( --custom

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS tokens,
    'hashflow' AS platform,
    _call_id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__hashflow_pools') }}
),  #}

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
{# 
all_pools_other AS (
    SELECT *
    FROM balancer
    UNION ALL
    SELECT *
    FROM curve
) #}

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
        token0,
        token1,
        c0.symbol AS token0_symbol,
        c1.symbol AS token1_symbol,
        c0.decimals AS token0_decimals,
        c1.decimals AS token1_decimals,
        OBJECT_CONSTRUCT_KEEP_NULL('token0',token0,'token1',token1) AS tokens,
        OBJECT_CONSTRUCT_KEEP_NULL('token0',token0_symbol,'token1',token1_symbol) AS symbols,
        OBJECT_CONSTRUCT_KEEP_NULL('token0',token0_decimals,'token1',token1_decimals) AS decimals,
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
        token0,
        token1,
        c0.symbol AS token0_symbol,
        c1.symbol AS token1_symbol,
        c0.decimals AS token0_decimals,
        c1.decimals AS token1_decimals,
        OBJECT_CONSTRUCT_KEEP_NULL('token0',token0,'token1',token1) AS tokens,
        OBJECT_CONSTRUCT_KEEP_NULL('token0',token0_symbol,'token1',token1_symbol) AS symbols,
        OBJECT_CONSTRUCT_KEEP_NULL('token0',token0_decimals,'token1',token1_decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_v3 p 
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
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