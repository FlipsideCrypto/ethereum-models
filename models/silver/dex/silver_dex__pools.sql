{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH univ2_sushi_pairs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS factory_address,
        event_name,
        event_inputs :pair :: STRING AS pool_address,
        event_inputs :token0 :: STRING AS token0,
        event_inputs :token1 :: STRING AS token1,
        _log_id,
        ingested_at,
        CASE
            WHEN contract_address = '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f' THEN 'UniswapV2'
            WHEN contract_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'Sushiswap'
        END AS platform
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac'
        )
        AND event_name = 'PairCreated'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(ingested_at)
    FROM
        {{ this }}
)
{% endif %}
),
uniswap_v3_pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS factory_address,
        event_name,
        event_inputs :fee :: INTEGER AS fee,
        event_inputs :pool :: STRING AS pool_address,
        event_inputs :tickSpacing :: INTEGER AS tickSpacing,
        event_inputs :token0 :: STRING AS token0,
        event_inputs :token1 :: STRING AS token1,
        _log_id,
        ingested_at,
        'UniswapV3' AS platform
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
        AND event_name = 'PoolCreated'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(ingested_at)
    FROM
        {{ this }}
),
contract_details AS (
    SELECT
        address,
        symbol,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
)
