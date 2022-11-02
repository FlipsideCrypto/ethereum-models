{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH lp_actions_base AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'
        AND topics [0] :: STRING IN (
            '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
            '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
        ) -- burn / mint

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
uni_pools AS (
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
lp_amounts AS (
    SELECT
        tx_hash,
        event_index,
        block_timestamp,
        block_number,
        'ethereum' AS blockchain,
        _log_id,
        A._inserted_timestamp,
        topics,
        segmented_data,
        CASE
            WHEN topics [0] :: STRING = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' THEN 'DECREASE_LIQUIDITY'
            WHEN topics [0] :: STRING = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' THEN 'INCREASE_LIQUIDITY'
        END AS action,
        contract_address AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS liquidity_provider,
        -- CHECK THIS!
        PUBLIC.udf_hex_to_int(
            's2c',
            topics [2] :: STRING
        ) :: FLOAT AS tick_lower,
        COALESCE(
            event_inputs :tickUpper :: STRING,
            PUBLIC.udf_hex_to_int(
                's2c',
                topics [3] :: STRING
            )
        ) :: FLOAT AS tick_upper,
        CASE
            WHEN topics [0] :: STRING = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' THEN PUBLIC.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
            WHEN topics [0] :: STRING = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' THEN PUBLIC.udf_hex_to_int(
                's2c',
                segmented_data [1] :: STRING
            )
        END AS amount0,
        CASE
            WHEN topics [0] :: STRING = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' THEN PUBLIC.udf_hex_to_int(
                's2c',
                segmented_data [3] :: STRING
            )
            WHEN topics [0] :: STRING = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' THEN PUBLIC.udf_hex_to_int(
                's2c',
                segmented_data [2] :: STRING
            )
        END AS amount1,
        amount0 / pow(
            10,
            token0_decimals
        ) AS amount0_adjusted,
        amount1 / pow(
            10,
            token1_decimals
        ) AS amount1_adjusted,
        pow(1.0001, (tick_lower)) / pow(10,(token1_decimals - token0_decimals)) AS price_lower_1_0,
        pow(1.0001, (tick_upper)) / pow(10,(token1_decimals - token0_decimals)) AS price_upper_1_0,
        pow(1.0001, -1 * (tick_upper)) / pow(10,(token0_decimals - token1_decimals)) AS price_lower_0_1,
        pow(1.0001, -1 * (tick_lower)) / pow(10,(token0_decimals - token1_decimals)) AS price_upper_0_1,
        pool_name,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        token1_decimals,
        token0_decimals,
        origin_to_address,
        origin_from_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        lp_actions_base A
        INNER JOIN uni_pools
        ON contract_address = pool_address
),
liquidity_info AS (
    SELECT
        tx_hash,
        CASE
            WHEN SUBSTR(
                input,
                0,
                10
            ) = '0xa34123a7' THEN 'DECREASE_LIQUIDITY'
            WHEN SUBSTR(
                input,
                0,
                10
            ) = '0x3c8a7d8d' THEN 'INCREASE_LIQUIDITY'
        END AS action,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        CASE
            WHEN SUBSTR(
                input,
                0,
                10
            ) = '0xa34123a7' THEN udf_hex_to_int(
                's2c',
                segmented_input [2] :: STRING
            )
            ELSE udf_hex_to_int(
                's2c',
                segmented_input [3] :: STRING
            )
        END AS liquidity,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                identifier ASC
        ) AS agg_id,
        to_address
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND TYPE = 'CALL'
        AND SUBSTR(
            input,
            0,
            10
        ) IN (
            '0x3c8a7d8d',
            '0xa34123a7'
        )
        AND tx_status = 'SUCCESS'

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
nf_info AS (
    SELECT
        tx_hash,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS nf_token_id,
        contract_address AS nf_position_manager_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'
        AND contract_address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
        AND topics [0] :: STRING IN (
            '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f',
            '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01'
        )

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
                lp_actions_base
        )
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        blockchain,
        block_number,
        block_timestamp,
        A.tx_hash AS tx_hash,
        A.action AS action,
        amount0_adjusted,
        amount1_adjusted,
        amount0_adjusted * p0.price AS amount0_usd,
        amount1_adjusted * p1.price AS amount1_usd,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        p0.price AS token0_price,
        p1.price AS token1_price,
        liquidity :: INTEGER AS liquidity,
        liquidity / pow(10, (token1_decimals + token0_decimals) / 2) AS liquidity_adjusted,
        CASE
            WHEN nf_position_manager_address IS NOT NULL THEN origin_from_address
            ELSE origin_to_address
        END AS liquidity_provider,
        nf_position_manager_address,
        nf_token_id,
        pool_address,
        pool_name,
        A.tick_lower AS tick_lower,
        A.tick_upper AS tick_upper,
        price_lower_1_0,
        price_upper_1_0,
        price_lower_0_1,
        price_upper_0_1,
        price_lower_1_0 * p1.price AS price_lower_1_0_usd,
        price_upper_1_0 * p1.price AS price_upper_1_0_usd,
        price_lower_0_1 * p0.price AS price_lower_0_1_usd,
        price_upper_0_1 * p0.price AS price_upper_0_1_usd,
        _log_id,
        _inserted_timestamp
    FROM
        lp_amounts A
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
        LEFT JOIN liquidity_info b
        ON A.tx_hash = b.tx_hash
        AND A.pool_address = b.to_address
        LEFT JOIN nf_info C
        ON A.tx_hash = C.tx_hash
        AND A.agg_id = C.agg_id
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
