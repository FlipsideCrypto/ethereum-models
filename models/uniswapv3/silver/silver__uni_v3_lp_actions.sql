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
        AND (
            (
                contract_address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
                AND topics [0] :: STRING IN (
                    '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f',
                    '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4'
                )
            )
            OR topics [0] :: STRING IN (
                '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
                '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
            ) -- burn / mint
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
lp_actions AS (
    SELECT
        tx_hash,
        contract_address AS nf_position_manager_address,
        event_index,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS nf_token_id,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS liquidity,
        liquidity / pow(
            10,
            18
        ) AS liquidity_adjusted,
        CASE
            WHEN topics [0] :: STRING = '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f' THEN event_index + 2
            WHEN topics [0] :: STRING = '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4' THEN event_index + 1
        END AS event_index_join
    FROM
        lp_actions_base
    WHERE
        contract_address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
),
token_actions AS (
    SELECT
        tx_hash,
        event_index,
        block_timestamp,
        block_number,
        'ethereum' AS blockchain,
        _log_id,
        _inserted_timestamp,
        CASE
            WHEN topics [0] :: STRING = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c' THEN 'INCREASE_LIQUIDITY'
            WHEN topics [0] :: STRING = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' THEN 'DECREASE_LIQUIDITY'
        END AS action,
        contract_address AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS liquidity_provider,
        COALESCE(
            event_inputs :tickLower :: STRING,
            PUBLIC.udf_hex_to_int(
                topics [2] :: STRING
            )
        ) :: FLOAT AS tick_lower,
        COALESCE(
            event_inputs :tickUpper :: STRING,
            PUBLIC.udf_hex_to_int(
                topics [3] :: STRING
            )
        ) :: FLOAT AS tick_upper,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS amount,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS amount0,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS amount1,
        pow(
            1.0001,
            tick_lower
        ) * 100 AS price_lower_1_0,
        pow(
            1.0001,
            tick_upper
        ) * 100 AS price_upper_1_0,
        pow(
            1.0001,
            -1 * tick_lower
        ) * 100 AS price_lower_0_1,
        pow(
            1.0001,
            -1 * tick_upper
        ) * 100 AS price_upper_0_1
    FROM
        lp_actions_base
    WHERE
        contract_address <> '0xc36442b4a4522e871399cd717abdd847ab11fe88'
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
        {{ ref('silver__uni_v3_pools') }}
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
)
