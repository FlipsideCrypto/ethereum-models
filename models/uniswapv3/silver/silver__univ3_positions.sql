{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::date']
) }}

WITH lp_events AS (

    SELECT
        blockchain,
        block_number,
        block_timestamp,
        tx_hash,
        liquidity_adjusted,
        liquidity_provider,
        nf_position_manager_address,
        nf_token_id,
        pool_address,
        pool_name,
        tick_upper,
        tick_lower,
        price_upper_1_0,
        price_lower_1_0,
        price_upper_0_1,
        price_lower_0_1,
        price_upper_1_0_usd,
        price_lower_1_0_usd,
        price_upper_0_1_usd,
        price_lower_0_1_usd,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__univ3_lp_actions') }}
    WHERE
        nf_position_manager_address IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
position_reads_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(
            's2c',
            function_input :: STRING
        ) AS nf_token_id,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) AS nonce,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) AS OPERATOR,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 27, 40)) AS token1,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) AS fee,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) AS tickLower,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) AS tickUpper,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) AS liquidity,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [8] :: STRING
        ) AS feeGrowthInside0LastX128,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [9] :: STRING
        ) AS feeGrowthInside1LastX128,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [10] :: STRING
        ) AS tokensOwed0,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [11] :: STRING
        ) AS tokensOwed1
    FROM
        {{ ref('bronze__univ3_position_reads') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
),
pool_data AS (
    SELECT
        token0_address,
        token1_address,
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
token_prices AS (
    SELECT
        HOUR,
        LOWER(token_address) AS token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                lp_events
        )
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        blockchain,
        A.block_number,
        block_timestamp,
        tx_hash,
        fee,
        fee / 10000 AS fee_percent,
        feeGrowthInside0LastX128 AS fee_growth_inside0_last_x128,
        feeGrowthInside1LastX128 AS fee_growth_inside1_last_x128,
        CASE
            WHEN fee_percent <> 0 THEN TRUE
            ELSE FALSE
        END AS is_active,
        COALESCE(
            liquidity_adjusted,
            liquidity / pow(10, (token1_decimals + token0_decimals) / 2)
        ) AS liquidity_adjusted,
        liquidity_provider,
        nf_position_manager_address,
        A.nf_token_id,
        A.pool_address,
        A.pool_name,
        tick_upper,
        tick_lower,
        price_upper_1_0,
        price_lower_1_0,
        price_upper_0_1,
        price_lower_0_1,
        price_upper_1_0_usd,
        price_lower_1_0_usd,
        price_upper_0_1_usd,
        price_lower_0_1_usd,
        tokensOwed0 / pow(
            10,
            token0_decimals
        ) AS tokens_owed0_adjusted,
        tokensOwed1 / pow(
            10,
            token1_decimals
        ) AS tokens_owed1_adjusted,
        tokens_owed0_adjusted * p0.price AS tokens_owed0_usd,
        tokens_owed1_adjusted * p1.price AS tokens_owed1_usd,
        A.token0_address,
        A.token1_address,
        A.token0_symbol,
        A.token1_symbol,
        _log_id,
        _inserted_timestamp
    FROM
        lp_events A
        LEFT JOIN position_reads_base b
        ON A.block_number = b.block_number
        AND A.nf_token_id = b.nf_token_id
        LEFT JOIN pool_data C
        ON A.pool_address = C.pool_address
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
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
