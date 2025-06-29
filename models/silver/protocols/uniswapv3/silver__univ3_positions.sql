{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::date'],
    tags = ['silver','curated','uniswap']
) }}

WITH lp_events AS (

    SELECT
        blockchain,
        block_number,
        block_timestamp,
        tx_hash,
        liquidity_provider,
        nf_position_manager_address,
        nf_token_id,
        pool_address,
        tick_upper,
        tick_lower,
        token0_address,
        token1_address,
        _log_id,
        _inserted_timestamp,
        event_index
    FROM
        {{ ref('silver__univ3_lp_actions') }}
    WHERE
        nf_token_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
position_reads_base AS (
    SELECT
        contract_address,
        block_number,
        utils.udf_hex_to_int(CONCAT('0x', function_input :: STRING)) :: STRING AS nf_token_id,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS nonce,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) AS OPERATOR,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 27, 40)) AS token1,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: FLOAT AS fee,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) :: FLOAT AS tickLower,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: FLOAT AS tickUpper,
        utils.udf_hex_to_int(
            segmented_data [7] :: STRING
        ) :: FLOAT AS liquidity,
        utils.udf_hex_to_int(
            segmented_data [8] :: STRING
        ) :: FLOAT AS feeGrowthInside0LastX128,
        utils.udf_hex_to_int(
            segmented_data [9] :: STRING
        ) :: FLOAT AS feeGrowthInside1LastX128,
        utils.udf_hex_to_int(
            segmented_data [10] :: STRING
        ) :: FLOAT AS tokensOwed0,
        utils.udf_hex_to_int(
            segmented_data [11] :: STRING
        ) :: FLOAT AS tokensOwed1
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature = '0x99fbab88'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
pool_data AS (
    SELECT
        token0_address,
        token1_address,
        fee_percent,
        tick_spacing,
        pool_address,
        token0_symbol,
        token1_symbol,
        token0_decimals,
        token1_decimals,
        pool_name
    FROM
        {{ ref('silver__univ3_pools') }}
),
FINAL AS (
    SELECT
        blockchain,
        A.block_number,
        block_timestamp,
        tx_hash,
        COALESCE(
            fee,
            0
        ) AS fee,
        COALESCE(
            fee,
            0
        ) / 10000 AS fee_percent,
        COALESCE(
            feeGrowthInside0LastX128,
            0
        ) AS fee_growth_inside0_last_x128,
        COALESCE(
            feeGrowthInside1LastX128,
            0
        ) AS fee_growth_inside1_last_x128,
        CASE
            WHEN fee_percent <> 0 THEN TRUE
            ELSE FALSE
        END AS is_active,
        liquidity,
        liquidity_provider,
        nf_position_manager_address,
        A.nf_token_id,
        A.pool_address,
        COALESCE(
            tick_upper,
            0
        ) AS tick_upper,
        COALESCE(
            tick_lower,
            0
        ) AS tick_lower,
        tokensOwed0,
        tokensOwed1,
        A.token0_address,
        A.token1_address,
        _log_id,
        _inserted_timestamp,
        event_index
    FROM
        lp_events A
        LEFT JOIN position_reads_base b
        ON A.block_number = b.block_number
        AND A.nf_token_id :: STRING = b.nf_token_id :: STRING
        LEFT JOIN pool_data C
        ON A.pool_address = C.pool_address
),
silver_positions AS (
    SELECT
        *
    FROM
        FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
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
                DISTINCT block_timestamp :: DATE
            FROM
                silver_positions
        )
)
SELECT
    blockchain,
    block_number,
    block_timestamp,
    tx_hash,
    A.fee_percent,
    fee_growth_inside0_last_x128,
    fee_growth_inside1_last_x128,
    is_active,
    COALESCE(
        liquidity / pow(10, (token1_decimals + token0_decimals) / 2),
        0
    ) AS liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    A.pool_address,
    pool_name,
    A.tick_upper,
    A.tick_lower,
    pow(1.0001, (tick_lower)) / pow(10,(token1_decimals - token0_decimals)) AS price_lower_1_0,
    pow(1.0001, (tick_upper)) / pow(10,(token1_decimals - token0_decimals)) AS price_upper_1_0,
    pow(1.0001, -1 * (tick_upper)) / pow(10,(token0_decimals - token1_decimals)) AS price_lower_0_1,
    pow(1.0001, -1 * (tick_lower)) / pow(10,(token0_decimals - token1_decimals)) AS price_upper_0_1,
    price_lower_1_0 * p1.price AS price_lower_1_0_usd,
    price_upper_1_0 * p1.price AS price_upper_1_0_usd,
    price_lower_0_1 * p0.price AS price_lower_0_1_usd,
    price_upper_0_1 * p0.price AS price_upper_0_1_usd,
    COALESCE(
        tokensOwed0 / pow(
            10,
            token0_decimals
        ),
        0
    ) AS tokens_owed0_adjusted,
    COALESCE(
        tokensOwed1 / pow(
            10,
            token1_decimals
        ),
        0
    ) AS tokens_owed1_adjusted,
    COALESCE(
        tokens_owed0_adjusted * p0.price,
        0
    ) AS tokens_owed0_usd,
    COALESCE(
        tokens_owed1_adjusted * p1.price,
        0
    ) AS tokens_owed1_usd,
    A.token0_address,
    A.token1_address,
    token0_symbol,
    token1_symbol,
    event_index,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS univ3_positions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver_positions A
    LEFT JOIN pool_data p
    ON A.pool_address = p.pool_address
    LEFT JOIN token_prices p0
    ON p0.token_address = A.token0_address
    AND p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    LEFT JOIN token_prices p1
    ON p1.token_address = A.token1_address
    AND p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
