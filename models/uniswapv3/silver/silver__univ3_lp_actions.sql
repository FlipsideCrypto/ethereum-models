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
            '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',
            '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f',
            '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4'
        ) -- burn / mint / IncreaseLiquidity / DecreaseLiquidity

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
-- pulls info for increases or decreases (mint / burn events) in liquidity
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
        CASE
            WHEN topics [0] :: STRING = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' THEN PUBLIC.udf_hex_to_int(
                's2c',
                segmented_data [0] :: STRING
            )
            WHEN topics [0] :: STRING = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' THEN PUBLIC.udf_hex_to_int(
                's2c',
                segmented_data [1] :: STRING
            )
        END AS amount,
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
        amount + amount0 + amount1 AS total_amount,
        ROW_NUMBER() over(
            PARTITION BY tx_hash,
            amount,
            amount0,
            amount1
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        lp_actions_base A
        INNER JOIN uni_pools
        ON contract_address = pool_address
    WHERE
        topics [0] :: STRING IN (
            '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
            '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
        )
),
-- pulls info for token IDs minted or burned, if applicable (not all positions have token IDs)
nf_info AS (
    SELECT
        tx_hash,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS nf_token_id,
        contract_address AS nf_position_manager_address,
        CASE
            WHEN topics [0] :: STRING = '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f' THEN 'INCREASE_LIQUIDITY'
            ELSE 'DECREASE_LIQUIDITY'
        END AS action,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS liquidity,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS amount0,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS amount1,
        event_index,
        ROW_NUMBER() over(
            PARTITION BY tx_hash,
            liquidity,
            amount0,
            amount1
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        lp_actions_base
    WHERE
        contract_address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
        AND topics [0] :: STRING IN (
            '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f',
            '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4'
        )
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
        A.amount :: INTEGER AS liquidity,
        liquidity / pow(10, (token1_decimals + token0_decimals) / 2) AS liquidity_adjusted,
        -- CASE
        --     WHEN nf_position_manager_address IS NOT NULL THEN origin_from_address
        --     ELSE liquidity_provider
        -- END AS
        liquidity_provider,
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
        A._log_id AS _log_id,
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
        LEFT JOIN nf_info C
        ON A.tx_hash = C.tx_hash
        AND A.amount = C.liquidity
        AND A.amount0 = C.amount0
        AND A.amount1 = C.amount1
        AND A.agg_id = C.agg_id
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
