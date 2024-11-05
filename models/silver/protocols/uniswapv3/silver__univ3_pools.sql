{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "created_block",
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['curated']
) }}

WITH created_pools AS (

    SELECT
        block_number AS created_block,
        block_timestamp AS created_time,
        tx_hash AS created_tx_hash,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        utils.udf_hex_to_int(
            's2c',
            topics [3] :: STRING
        ) :: INTEGER AS fee,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: INTEGER AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
        _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        AND contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
initial_info AS (
    SELECT
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS init_sqrtPriceX96,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING)) :: FLOAT AS init_tick,
        pow(
            1.0001,
            init_tick
        ) AS init_price_1_0_unadj
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
silver_pools as (
    SELECT
        created_block,
        created_time,
        created_tx_hash,
        token0_address,
        token1_address,
        fee :: INTEGER AS fee,
        (
            fee / 10000
        ) :: FLOAT AS fee_percent,
        tick_spacing,
        pool_address,
        init_sqrtPriceX96,
        COALESCE(
            init_tick,
            0
        ) AS init_tick,
        init_price_1_0_unadj,
        _inserted_timestamp
    FROM created_pools
    LEFT JOIN initial_info
        ON pool_address = contract_address
),
contracts AS (
    SELECT
        LOWER(address) AS address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        decimals IS NOT NULL
),

token_prices AS (
    SELECT
        HOUR,
        LOWER(token_address) AS token_address,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT created_time :: DATE
            FROM
                silver_pools 
        )
)
SELECT
    'ethereum' AS blockchain,
    created_block,
    created_time,
    created_tx_hash,
    '0x1f98431c8ad98523631ae4a59f267346ea31f984' AS factory_address,
    token0_address,
    token1_address,
    c0.symbol AS token0_symbol,
    c1.symbol AS token1_symbol,
    c0.name AS token0_name,
    c1.name AS token1_name,
    c0.decimals AS token0_decimals,
    c1.decimals AS token1_decimals,
    p0.price AS token0_price,
    p1.price AS token1_price,
    fee,
    fee_percent,
    COALESCE(
        init_price_1_0_unadj / pow(
            10,
            token1_decimals - token0_decimals
        ),
        0
    ) AS init_price_1_0,
    init_price_1_0 * token1_price AS init_price_1_0_usd,
    init_tick,
    tick_spacing,
    div0(
        token1_price,
        token0_price
    ) AS usd_ratio,
    pool_address,
    CONCAT(
        token0_symbol,
        '-',
        token1_symbol,
        ' ',
        fee,
        ' ',
        tick_spacing
    ) AS pool_name,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS univ3_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM silver_pools p 
LEFT JOIN contracts c0
    ON c0.address = p.token0_address
LEFT JOIN contracts c1
    ON c1.address = p.token1_address
LEFT JOIN token_prices p0
    ON p0.token_address = p.token0_address
    AND p0.hour = DATE_TRUNC('hour',created_time)
LEFT JOIN token_prices p1
    ON p1.token_address = p.token1_address
    AND p1.hour = DATE_TRUNC('hour',created_time)
