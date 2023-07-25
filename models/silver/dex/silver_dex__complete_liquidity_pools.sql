{{ config(
  materialized = 'incremental',
  unique_key = "_log_id",
  cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS ( --join core dim_contracts in gold view instead if possible

  SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata
  FROM
    {{ ref('silver__contracts') }}
),

uni_sushi_v2_v3 AS (
    creation_block AS block_number,
    creation_time AS block_timestamp,
    creation_tx AS tx_hash,
    factory_address AS contract_address,
    CASE
        WHEN pool_name IS NULL AND platform = 'sushiswap' 
            THEN c0.symbol || '-' || c1.symbol || ' SLP'
        WHEN pool_name IS NULL AND platform = 'uniswap-v2' 
            THEN c0.symbol || '-' || c1.symbol || ' UNI-V2 LP'
        WHEN pool_name IS NULL AND platform = 'uniswap-v3' 
            THEN c0.symbol || '-' || c1.symbol || ' ' || fee || ' ' || tickSpacing || ' UNI-V3 LP'
        ELSE pool_name
    END AS pool_name,
    pool_address,
    platform,
    token0,
    token1,
    c0.symbol AS token0_symbol,
    c1.symbol AS token1_symbol,
    c0.decimals AS token0_decimals,
    c1.decimals AS token1_decimals,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pools') }} p 
LEFT JOIN {{ ref('silver__contracts') }} c0
    ON c0.address = p.token0
LEFT JOIN {{ ref('silver__contracts') }} c1
    ON c1.address = p.token1
),

shibaswap AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    CONCAT(LEAST(c0.symbol, c1.symbol), '-', GREATEST(c0.symbol, c1.symbol)) AS pool_name,
    pool_address,
    token0,
    token1,
    c0.symbol AS token0_symbol,
    c1.symbol AS token1_symbol,
    c0.decimals AS token0_decimals,
    c1.decimals AS token1_decimals,
    platform,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__shibaswap_pools') }}
LEFT JOIN {{ ref('silver__contracts') }} c0
    ON c0.address = p.token0
LEFT JOIN {{ ref('silver__contracts') }} c1
    ON c1.address = p.token1
)
--union all similar then join contracts
--union all custom