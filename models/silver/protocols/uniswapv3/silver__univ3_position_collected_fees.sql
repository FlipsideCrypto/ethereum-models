{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH all_collected AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        fact_event_logs_id,
        inserted_timestamp,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND tx_succeeded
        AND event_removed = 'false'
        AND topics [0] :: STRING IN (
            '0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0',
            '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01'
        )

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
collected_base AS (
    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS vault_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS owner,
        utils.udf_hex_to_int(
            's2c',
            topics [2] :: STRING
        ) AS tick_lower,
        utils.udf_hex_to_int(
            's2c',
            topics [3] :: STRING
        ) AS tick_upper,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) AS amount0,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) AS amount1
    FROM
        all_collected
    WHERE
        topics [0] :: STRING = '0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0'
),
nf_token_id_base AS (
    SELECT
        tx_hash,
        contract_address AS nf_position_manager_address,
        utils.udf_hex_to_int(
            's2c',
            topics [1] :: STRING
        ) AS nf_token_id,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) AS liquidity_provider,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) AS amount0,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) AS amount1,
        event_index,
        event_index - 1 AS event_index_join
    FROM
        all_collected
    WHERE
        topics [0] :: STRING = '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01'
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
silver_positions AS (
    SELECT
        'ethereum' AS blockchain,
        b.block_number AS block_number,
        b.block_timestamp AS block_timestamp,
        b.tx_hash AS tx_hash,
        b.event_index AS event_index,
        b.contract_address AS pool_address,
        b.origin_from_address AS liquidity_provider,
        nf_token_id,
        CASE
            WHEN nf_token_id IS NULL THEN vault_address
            ELSE nf_position_manager_address
        END AS nf_position_manager_address,
        token0_address,
        token1_address,
        b.amount0,
        b.amount1,
        b.tick_lower,
        b.tick_upper,
        _inserted_timestamp,
        _log_id
    FROM
        collected_base b
        LEFT JOIN nf_token_id_base
        ON b.tx_hash = nf_token_id_base.tx_hash
        AND b.event_index = nf_token_id_base.event_index_join
        LEFT JOIN pool_data
        ON b.contract_address = pool_data.pool_address
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
    event_index,
    A.pool_address,
    pool_name,
    liquidity_provider,
    nf_token_id,
    nf_position_manager_address,
    token0_symbol,
    token1_symbol,
    amount0,
    amount1,
    amount0 / pow(
        10,
        token0_decimals
    ) AS amount0_adjusted,
    amount1 / pow(
        10,
        token1_decimals
    ) AS amount1_adjusted,
    ROUND(
        amount0_adjusted * p0.price,
        2
    ) AS amount0_usd,
    ROUND(
        amount1_adjusted * p1.price,
        2
    ) AS amount1_usd,
    tick_lower,
    tick_upper,
    pow(
        1.0001,
        tick_lower
    ) / pow(10,(token1_decimals - token0_decimals)) AS price_lower,
    pow(
        1.0001,
        tick_upper
    ) / pow(10,(token1_decimals - token0_decimals)) AS price_upper,
    ROUND(
        price_lower * p1.price,
        2
    ) AS price_lower_usd,
    ROUND(
        price_upper * p1.price,
        2
    ) AS price_upper_usd,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS univ3_position_collected_fees_id,
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
