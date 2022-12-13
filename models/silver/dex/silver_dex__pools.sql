{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = 'pool_address',
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
            WHEN contract_address = '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f' THEN 'uniswap-v2'
            WHEN contract_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'sushiswap'
        END AS platform,
        1 AS model_weight,
        'events' AS model_name
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
        'uniswap-v3' AS platform,
        1 AS model_weight,
        'events' AS model_name
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
)
{% endif %}
),
legacy_pipeline AS (
    SELECT
        creation_time,
        creation_tx,
        factory_address,
        pool_address,
        pool_name,
        token0,
        token1,
        platform,
        tokens,
        NULL AS creation_block,
        NULL AS event_name,
        NULL AS fee,
        NULL AS tickSpacing,
        NULL AS _log_id,
        NULL AS ingested_at,
        2 AS model_weight,
        'legacy' AS model_name
    FROM
        {{ source(
            'flipside_gold_ethereum',
            'dex_liquidity_pools'
        ) }}
),
union_ctes AS (
    SELECT
        block_number AS creation_block,
        block_timestamp AS creation_time,
        tx_hash AS creation_tx,
        factory_address,
        platform,
        event_name,
        pool_address,
        NULL AS pool_name,
        token0,
        token1,
        NULL AS fee,
        NULL AS tickSpacing,
        ARRAY_CONSTRUCT(
            token0,
            token1
        ) AS tokens,
        _log_id,
        ingested_at,
        model_weight,
        model_name
    FROM
        univ2_sushi_pairs
    UNION ALL
    SELECT
        block_number AS creation_block,
        block_timestamp AS creation_time,
        tx_hash AS creation_tx,
        factory_address,
        platform,
        event_name,
        pool_address,
        NULL AS pool_name,
        token0,
        token1,
        fee,
        tickSpacing,
        ARRAY_CONSTRUCT(
            token0,
            token1
        ) AS tokens,
        _log_id,
        ingested_at,
        model_weight,
        model_name
    FROM
        uniswap_v3_pools
    UNION ALL
    SELECT
        creation_block,
        creation_time,
        creation_tx,
        factory_address,
        platform,
        event_name,
        pool_address,
        pool_name,
        token0,
        token1,
        fee,
        tickSpacing,
        tokens,
        _log_id,
        ingested_at,
        model_weight,
        model_name
    FROM
        legacy_pipeline
),
dedup_pools AS (
    SELECT
        *
    FROM
        union_ctes
    WHERE
        pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY pool_address
    ORDER BY
        model_weight ASC)) = 1
),
contract_details AS (
    SELECT
        LOWER(address) AS token_address,
        symbol,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        address IN (
            SELECT
                DISTINCT token0
            FROM
                dedup_pools
        )
        OR address IN (
            SELECT
                DISTINCT token1
            FROM
                dedup_pools
        )
),
FINAL AS (
    SELECT
        creation_block,
        creation_time,
        creation_tx,
        factory_address,
        platform,
        event_name,
        pool_address,
        CASE
            WHEN pool_name IS NULL
            AND platform = 'sushiswap' THEN contract0.symbol || '-' || contract1.symbol || ' SLP'
            WHEN pool_name IS NULL
            AND platform = 'uniswap-v2' THEN contract0.symbol || '-' || contract1.symbol || ' UNI-V2 LP'
            WHEN pool_name IS NULL
            AND platform = 'uniswap-v3' THEN contract0.symbol || '-' || contract1.symbol || ' ' || fee || ' ' || tickSpacing || ' UNI-V3 LP'
            WHEN platform = 'curve' THEN pool_name
            ELSE pool_name
        END AS pool_name,
        token0 AS token0_address,
        contract0.symbol AS token0_symbol,
        contract0.decimals AS token0_decimals,
        token1 AS token1_address,
        contract1.symbol AS token1_symbol,
        contract1.decimals AS token1_decimals,
        fee,
        tickSpacing,
        _log_id,
        ingested_at,
        tokens,
        model_name
    FROM
        dedup_pools
        LEFT JOIN contract_details AS contract0
        ON contract0.token_address = dedup_pools.token0
        LEFT JOIN contract_details AS contract1
        ON contract1.token_address = dedup_pools.token1
)
SELECT
    creation_block,
    creation_time,
    creation_tx,
    factory_address,
    platform,
    event_name,
    pool_address,
    pool_name,
    token0_address,
    token0_symbol,
    token0_decimals,
    token1_address,
    token1_symbol,
    token1_decimals,
    fee,
    tickSpacing,
    _log_id,
    ingested_at,
    tokens,
    model_name
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    ingested_at DESC nulls last)) = 1
