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
        'PairCreated' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x',SUBSTRING(segmented_data[0] :: STRING,25,40)) AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
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
        AND topics[0] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'

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
        'PoolCreated' AS event_name,
        PUBLIC.udf_hex_to_int(topics [3] :: STRING) :: INTEGER AS fee,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x',SUBSTRING(segmented_data[1] :: STRING,25,40)) AS pool_address,
        PUBLIC.udf_hex_to_int(segmented_data[0]::STRING) :: INTEGER AS tickSpacing,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        _log_id,
        ingested_at,
        'uniswap-v3' AS platform,
        1 AS model_weight,
        'events' AS model_name
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
        AND topics[0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'

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
        case when factory_address = '0x115934131916c8b277dd010ee02de363c09d037c' then 'shibaswap'
        else platform end as platform,
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
    where factory_address in ('0x0959158b6040d32d04c301a72cbfd6b39e21c9ae', '0x115934131916c8b277dd010ee02de363c09d037c','0x90e00ace148ca3b23ac1bc8c240c2a7dd9c2d7f5','0xfd6f33a0509ec67defc500755322abd9df1bd5b8')

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
    union_ctes
WHERE
    pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    model_weight ASC)) = 1


--draft
-- contract_details AS (
--     SELECT
--         LOWER(address) AS token_address,
--         symbol,
--         decimals
--     FROM
--         {{ ref('core__dim_contracts') }}
--     WHERE
--         address IN (
--             SELECT
--                 DISTINCT token0
--             FROM
--                 dedup_pools
--         )
--         OR address IN (
--             SELECT
--                 DISTINCT token1
--             FROM
--                 dedup_pools
--         )
-- ),
-- FINAL AS (
--     SELECT
--         creation_block,
--         creation_time,
--         creation_tx,
--         factory_address,
--         platform,
--         event_name,
--         pool_address,
--         CASE
--             WHEN pool_name IS NULL
--             AND platform = 'sushiswap' THEN contract0.symbol || '-' || contract1.symbol || ' SLP'
--             WHEN pool_name IS NULL
--             AND platform = 'uniswap-v2' THEN contract0.symbol || '-' || contract1.symbol || ' UNI-V2 LP'
--             WHEN pool_name IS NULL
--             AND platform = 'uniswap-v3' THEN contract0.symbol || '-' || contract1.symbol || ' ' || fee || ' ' || tickSpacing || ' UNI-V3 LP'
--             WHEN platform = 'curve' THEN pool_name
--             ELSE pool_name
--         END AS pool_name,
--         token0 AS token0_address,
--         contract0.symbol AS token0_symbol,
--         contract0.decimals AS token0_decimals,
--         token1 AS token1_address,
--         contract1.symbol AS token1_symbol,
--         contract1.decimals AS token1_decimals,
--         fee,
--         tickSpacing,
--         _log_id,
--         ingested_at,
--         tokens,
--         model_name
--     FROM
--         dedup_pools
--         LEFT JOIN contract_details AS contract0
--         ON contract0.token_address = dedup_pools.token0
--         LEFT JOIN contract_details AS contract1
--         ON contract1.token_address = dedup_pools.token1
-- )
-- SELECT
--     creation_block,
--     creation_time,
--     creation_tx,
--     factory_address,
--     platform,
--     event_name,
--     pool_address,
--     pool_name,
--     token0_address,
--     token0_symbol,
--     token0_decimals,
--     token1_address,
--     token1_symbol,
--     token1_decimals,
--     fee,
--     tickSpacing,
--     _log_id,
--     ingested_at,
--     tokens,
--     model_name
-- FROM
--     FINAL qualify(ROW_NUMBER() over(PARTITION BY pool_address
-- ORDER BY
--     ingested_at DESC nulls last)) = 1
