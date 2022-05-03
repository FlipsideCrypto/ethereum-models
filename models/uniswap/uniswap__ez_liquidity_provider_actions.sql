{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH mints_and_burns AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        ingested_at,
        event_index,
        contract_address AS pool_address,
        contract_name,
        event_inputs,
        event_name,
        topics,
        DATA,
        event_removed
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (
            event_name = 'Mint'
            AND event_inputs :owner IS NOT NULL
            AND event_inputs :tickLower IS NOT NULL
            AND event_inputs :tickUpper IS NOT NULL
            AND event_inputs :sender IS NOT NULL
            AND event_inputs :amount IS NOT NULL
            AND event_inputs :amount0 IS NOT NULL
            AND event_inputs :amount1 IS NOT NULL
        )
        OR (
            event_name = 'Burn'
            AND event_inputs :owner IS NOT NULL
            AND event_inputs :tickLower IS NOT NULL
            AND event_inputs :tickUpper IS NOT NULL
            AND event_inputs :amount IS NOT NULL
            AND event_inputs :amount0 IS NOT NULL
            AND event_inputs :amount1 IS NOT NULL
        )
        
{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(ingested_at) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
change_liquidity_events AS (
    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        ingested_at,
        event_index,
        contract_address,
        contract_name,
        event_inputs,
        topics,
        DATA,
        event_name,
        event_removed
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (
            (
                event_name = 'IncreaseLiquidity'
                OR event_name = 'DecreaseLiquidity'
            )
            AND event_inputs :amount0 IS NOT NULL
            AND event_inputs :amount1 IS NOT NULL
            AND event_inputs :liquidity IS NOT NULL
            AND event_inputs :tokenId IS NOT NULL
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(ingested_at) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
liquidity_changes_with_addresses AS (
    SELECT
        cl._log_id,
        cl.block_number,
        cl.block_timestamp,
        cl.tx_hash,
        cl.ingested_at,
        cl.event_index,
        cl.event_name,
        cl.contract_name,
        cl.event_inputs,
        cl.topics,
        cl.data,
        cl.event_removed,
        t.from_address,
        t.to_address
    FROM
        change_liquidity_events cl
        LEFT JOIN ethereum.silver.transactions t
        ON cl.tx_hash = t.tx_hash
),
liquidity_actions_unadjusted AS (
    SELECT
        cl._log_id,
        cl.block_number,
        cl.block_timestamp,
        cl.tx_hash,
        cl.event_inputs :tokenId :: INT AS tokenId,
        COALESCE(
            cl.event_inputs :liquidity,
            0
        ) :: INT AS liquidity,
        mab.event_inputs :owner :: STRING AS owner,
        mab.event_inputs :sender :: STRING AS sender,
        mab.event_inputs :amount :: STRING AS amount,
        mab.event_inputs :amount0 :: STRING AS amount0,
        mab.event_inputs :amount1 :: STRING AS amount1,
        mab.event_inputs :tickLower :: STRING AS tick_lower,
        mab.event_inputs :tickUpper :: STRING AS tick_upper,
        cl.ingested_at,
        cl.event_name AS action,
        cl.from_address,
        cl.to_address,
        cl.from_address AS liquidity_provider,
        cl.to_address AS nf_position_manager_address,
        mab.pool_address AS pool_address
    FROM
        liquidity_changes_with_addresses cl
        LEFT JOIN mints_and_burns mab
        ON cl.tx_hash = mab.tx_hash
        AND mab.event_name IN (
            'Mint',
            'Burn'
        )
        AND cl.event_inputs :amount0 = mab.event_inputs :amount0
        AND cl.event_inputs :amount1 = mab.event_inputs :amount1
        AND cl.event_index IN (
            mab.event_index + 1,
            mab.event_index + 2
        )
    WHERE
        cl.event_name IN (
            'IncreaseLiquidity',
            'DecreaseLiquidity'
        )
),
liquidity_actions_adjusted AS (
    SELECT
        lau.block_number,
        lau.block_timestamp,
        lau.tx_hash,
        lau.action,
        lau.pool_address,
        lau.tokenId AS nf_token_id,
        lau.liquidity_provider,
        lau.nf_position_manager_address,
        up.init_sqrt_price_x96 AS sqrt_price_x96,
        lau.liquidity * (
            SQRT(
                (
                    up.init_sqrt_price_x96 * up.init_sqrt_price_x96
                ) / pow(
                    2,
                    192
                )
            )
        ) AS virtual_reserves_token0,
        lau.liquidity * (
            SQRT(
                (
                    up.init_sqrt_price_x96 * up.init_sqrt_price_x96
                ) / pow(
                    2,
                    192
                )
            )
        ) AS virtual_reserves_token1,
        virtual_reserves_token0 / pow(
            10,
            up.token0_decimals
        ) AS virtual_reserves_token0_adjusted,
        virtual_reserves_token1 / pow(
            10,
            up.token1_decimals
        ) AS virtual_reserves_token1_adjusted,
        lau.liquidity,
        SQRT(
            virtual_reserves_token0_adjusted * virtual_reserves_token1_adjusted
        ) AS liquidity_adjusted,
        lau.amount,
        lau.amount0,
        (lau.amount0 / power(10, up.token0_decimals)) AS amount0_adjusted,
        lau.amount1,
        (lau.amount1 / power(10, up.token1_decimals)) AS amount1_adjusted,
        lau.tick_lower,
        lau.tick_upper,
        lau.ingested_at,
        lau._log_id,
        up.token0_decimals,
        up.token1_decimals,
        up.token0,
        up.token1,
        up.token0_symbol,
        up.token1_symbol,
        up.pool_name
    FROM
        liquidity_actions_unadjusted lau
        LEFT JOIN {{ ref('silver_dex__pools') }}
        up
        ON lau.pool_address = up.pool_address
),
lp_actions_bronze AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        action,
        pool_address,
        nf_token_id,
        liquidity_provider,
        nf_position_manager_address,
        sqrt_price_x96,
        virtual_reserves_token0,
        virtual_reserves_token1,
        virtual_reserves_token0_adjusted,
        virtual_reserves_token1_adjusted,
        liquidity,
        liquidity_adjusted,
        amount,
        amount0,
        amount0_adjusted,
        amount1,
        amount1_adjusted,
        tick_lower,
        tick_upper,
        token0_decimals,
        token1_decimals,
        token0,
        token1,
        token0_symbol,
        token1_symbol,
        pool_name,
        ingested_at,
        _log_id
    FROM
        liquidity_actions_adjusted
),
prices AS (
    SELECT
        DATE_TRUNC(
            'day',
            HOUR
        ) AS HOUR,
        LOWER(token_address) AS token_address,
        decimals,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        lp_actions_bronze
)
{% else %}
    AND HOUR :: DATE >= '2020-05-05'
{% endif %}
GROUP BY
    HOUR,
    token_address,
    decimals
)
SELECT
    lab.block_number,
    lab.block_timestamp,
    lab.tx_hash,
    lab.action,
    lab.amount0_adjusted AS amount0_adjusted,
    lab.amount1_adjusted AS amount1_adjusted,
    CASE
        WHEN amount0_adjusted IS NOT NULL
        AND prices_0.price IS NOT NULL THEN amount0_adjusted * prices_0.price
        ELSE NULL
    END AS amount0_usd,
    CASE
        WHEN amount1_adjusted IS NOT NULL
        AND prices_1.price IS NOT NULL THEN amount1_adjusted * prices_1.price
        ELSE NULL
    END AS amount1_usd,
    lab.token0 AS token0_address,
    lab.token1 AS token1_address,
    lab.token0_symbol,
    lab.token1_symbol,
    prices_0.price AS token0_price,
    prices_1.price AS token1_price,
    lab.liquidity AS liquidity,
    lab.liquidity_adjusted,
    lab.virtual_reserves_token0,
    lab.virtual_reserves_token1,
    lab.virtual_reserves_token0_adjusted,
    lab.virtual_reserves_token1_adjusted,
    lab.liquidity_provider,
    lab.nf_position_manager_address,
    lab.nf_token_id,
    lab.pool_address,
    lab.pool_name,
    lab.tick_lower,
    lab.tick_upper,
    CASE
        WHEN lab.tick_lower IS NOT NULL
        AND lab.token1_decimals IS NOT NULL
        AND lab.token0_decimals IS NOT NULL THEN pow(
            1.0001,
            lab.tick_lower
        ) / pow(
            10,
            lab.token1_decimals - lab.token0_decimals
        )
        ELSE NULL
    END AS price_lower_1_0,
    CASE
        WHEN lab.tick_upper IS NOT NULL
        AND lab.token1_decimals IS NOT NULL
        AND lab.token0_decimals IS NOT NULL THEN pow(
            1.0001,
            lab.tick_upper
        ) / pow(
            10,
            lab.token1_decimals - lab.token0_decimals
        )
        ELSE NULL
    END AS price_upper_1_0,
    1 / price_upper_1_0 AS price_lower_0_1,
    1 / price_lower_1_0 AS price_upper_0_1,
    CASE
        WHEN price_lower_1_0 IS NOT NULL
        AND prices_1.price IS NOT NULL THEN price_lower_1_0 * prices_1.price
        ELSE NULL
    END AS price_lower_1_0_usd,
    CASE
        WHEN price_upper_1_0 IS NOT NULL
        AND prices_1.price IS NOT NULL THEN price_upper_1_0 * prices_1.price
        ELSE NULL
    END AS price_upper_1_0_usd,
    CASE
        WHEN price_lower_0_1 IS NOT NULL
        AND prices_0.price IS NOT NULL THEN price_lower_0_1 * prices_0.price
        ELSE NULL
    END AS price_lower_0_1_usd,
    CASE
        WHEN price_upper_0_1 IS NOT NULL
        AND prices_0.price IS NOT NULL THEN price_upper_0_1 * prices_0.price
        ELSE NULL
    END AS price_upper_0_1_usd,
    lab.ingested_at,
    lab._log_id
FROM
    lp_actions_bronze lab
    LEFT OUTER JOIN prices prices_0
    ON prices_0.hour = DATE_TRUNC(
        'day',
        lab.block_timestamp
    )
    AND lab.token0 = prices_0.token_address
    LEFT OUTER JOIN prices prices_1
    ON prices_1.hour = DATE_TRUNC(
        'day',
        lab.block_timestamp
    )
    AND lab.token1 = prices_1.token_address
