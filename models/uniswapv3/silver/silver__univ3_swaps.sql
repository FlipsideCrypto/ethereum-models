{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_swaps AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS recipient,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: FLOAT AS amount0_unadj,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: FLOAT AS amount1_unadj,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) :: FLOAT AS sqrtPriceX96,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) :: FLOAT AS liquidity,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: FLOAT AS tick
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND topics [0] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

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
pool_data AS (
    SELECT
        token0_address,
        token1_address,
        fee,
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
                base_swaps
        )
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        'ethereum' AS blockchain,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS pool_address,
        pool_name,
        recipient,
        sender,
        tick,
        liquidity,
        COALESCE(
            liquidity / pow(
                10,
                (
                    (
                        token0_decimals + token1_decimals
                    ) / 2
                )
            ),
            0
        ) AS liquidity_adjusted,
        event_index,
        amount0_unadj / pow(
            10,
            token0_decimals
        ) AS amount0_adjusted,
        amount1_unadj / pow(
            10,
            token1_decimals
        ) AS amount1_adjusted,
        COALESCE(div0(ABS(amount1_adjusted), ABS(amount0_adjusted)), 0) AS price_1_0,
        COALESCE(div0(ABS(amount0_adjusted), ABS(amount1_adjusted)), 0) AS price_0_1,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        p0.price AS token0_price,
        p1.price AS token1_price,
        CASE
            WHEN token0_decimals IS NOT NULL THEN ROUND(
                token0_price * amount0_adjusted,
                2
            )
        END AS amount0_usd,
        CASE
            WHEN token1_decimals IS NOT NULL THEN ROUND(
                token1_price * amount1_adjusted,
                2
            )
        END AS amount1_usd,
        _log_id,
        _inserted_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        amount0_unadj,
        amount1_unadj,
        token0_decimals,
        token1_decimals
    FROM
        base_swaps
        LEFT JOIN pool_data
        ON pool_data.pool_address = base_swaps.contract_address
        LEFT JOIN token_prices p0
        ON p0.token_address = token0_address
        AND p0.hour = DATE_TRUNC(
            'hour',
            block_timestamp
        )
        LEFT JOIN token_prices p1
        ON p1.token_address = token1_address
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
