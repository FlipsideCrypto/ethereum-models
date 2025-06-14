{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH univ2_sushi_pairs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS factory_address,
        'PairCreated' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTRING(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        CASE
            WHEN contract_address = '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f' THEN 'uniswap-v2'
            WHEN contract_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'sushiswap'
        END AS platform,
        1 AS model_weight,
        'events' AS model_name
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac'
        )
        AND topics [0] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
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
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS fee,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTRING(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS tickSpacing,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        'uniswap-v3' AS platform,
        1 AS model_weight,
        'events' AS model_name
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
        AND topics [0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_pools AS (
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
        _inserted_timestamp,
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
        _inserted_timestamp,
        model_weight,
        model_name
    FROM
        uniswap_v3_pools
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
    _inserted_timestamp,
    model_weight,
    model_name
FROM
    all_pools
WHERE
    pool_address IS NOT NULL 
    AND platform IN (
        'uniswap-v2',
        'uniswap-v3',
        'sushiswap'
        )
qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    model_weight ASC)) = 1
