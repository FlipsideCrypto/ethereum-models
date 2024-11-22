{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_swaps AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS recipient,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: FLOAT AS amount0_unadj,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: FLOAT AS amount1_unadj,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) :: FLOAT AS sqrtPriceX96,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) :: FLOAT AS liquidity,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: FLOAT AS tick
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
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
silver_swaps AS (
    SELECT
        'ethereum' AS blockchain,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS pool_address,
        recipient,
        sender,
        fee,
        tick,
        tick_spacing,
        liquidity,
        event_index,
        token0_address,
        token1_address,
        _log_id,
        _inserted_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        amount0_unadj,
        amount1_unadj
    FROM
        base_swaps
        INNER JOIN pool_data
        ON pool_data.pool_address = base_swaps.contract_address
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
                silver_swaps
        )
),
FINAL AS (
    SELECT
        s.blockchain,
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.pool_address,
        p.pool_name,
        recipient,
        sender,
        tick,
        s.fee,
        liquidity,
        amount0_unadj,
        amount1_unadj,
        COALESCE(
            liquidity / pow(10,((token0_decimals + token1_decimals) / 2)),
            0
        ) AS liquidity_adjusted,
        event_index,
        amount0_unadj / pow(
            10,
            COALESCE(
                token0_decimals,
                18
            )
        ) AS amount0_adjusted,
        amount1_unadj / pow(
            10,
            COALESCE(
                token1_decimals,
                18
            )
        ) AS amount1_adjusted,
        COALESCE(div0(ABS(amount1_adjusted), ABS(amount0_adjusted)), 0) AS price_1_0,
        COALESCE(div0(ABS(amount0_adjusted), ABS(amount1_adjusted)), 0) AS price_0_1,
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
        s.token0_address,
        s.token1_address,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        token0_symbol,
        token1_symbol,
        _log_id,
        _inserted_timestamp
    FROM
        silver_swaps s
        LEFT JOIN pool_data p
        ON s.pool_address = p.pool_address
        LEFT JOIN token_prices p0
        ON p0.token_address = s.token0_address
        AND p0.hour = DATE_TRUNC(
            'hour',
            block_timestamp
        )
        LEFT JOIN token_prices p1
        ON p1.token_address = s.token1_address
        AND p1.hour = DATE_TRUNC(
            'hour',
            block_timestamp
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS univ3_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
