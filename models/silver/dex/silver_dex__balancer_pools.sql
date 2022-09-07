{{ config(
    materialized = 'incremental',
    unique_key = "pool_address",
) }}

WITH vault_creation AS (

    SELECT
        tx_hash,
        event_inputs :poolId :: STRING AS poolId,
        LOWER(
            event_inputs :tokens [0] :: STRING
        ) AS token0,
        LOWER(
            event_inputs :tokens [1] :: STRING
        ) AS token1,
        LOWER(
            event_inputs :tokens [2] :: STRING
        ) AS token2,
        LOWER(
            event_inputs :tokens [3] :: STRING
        ) AS token3,
        LOWER(
            event_inputs :tokens [4] :: STRING
        ) AS token4,
        LOWER(
            event_inputs :tokens [5] :: STRING
        ) AS token5,
        LOWER(
            event_inputs :tokens [6] :: STRING
        ) AS token6,
        LOWER(
            event_inputs :tokens [7] :: STRING
        ) AS token7,
        event_inputs :tokens AS token_array,
        SUBSTR(
            event_inputs :poolId :: STRING,
            0,
            42
        ) AS pool_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        event_name = 'TokensRegistered'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
contracts AS (
    SELECT
        LOWER(address) AS address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
),
join_meta AS (
    SELECT
        tx_hash,
        poolId,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        token_array,
        c0.symbol AS token0_symbol,
        c0.decimals AS token0_decimals,
        c1.symbol AS token1_symbol,
        c1.decimals AS token1_decimals,
        c2.symbol AS token2_symbol,
        c2.decimals AS token2_decimals,
        c3.symbol AS token3_symbol,
        c3.decimals AS token3_decimals,
        c4.symbol AS token4_symbol,
        c4.decimals AS token4_decimals,
        c5.symbol AS token5_symbol,
        c5.decimals AS token5_decimals,
        c6.symbol AS token6_symbol,
        c6.decimals AS token6_decimals,
        c7.symbol AS token7_symbol,
        c7.decimals AS token7_decimals,
        pool_address,
        _inserted_timestamp
    FROM
        vault_creation
        LEFT JOIN contracts c0
        ON token0 = c0.address
        LEFT JOIN contracts c1
        ON token1 = c1.address
        LEFT JOIN contracts c2
        ON token2 = c2.address
        LEFT JOIN contracts c3
        ON token3 = c3.address
        LEFT JOIN contracts c4
        ON token4 = c4.address
        LEFT JOIN contracts c5
        ON token5 = c5.address
        LEFT JOIN contracts c6
        ON token6 = c6.address
        LEFT JOIN contracts c7
        ON token7 = c7.address
),
FINAL AS (
    SELECT
        tx_hash,
        poolId,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        token_array,
        CASE
            WHEN token0_symbol IS NULL THEN ''
            ELSE token0_symbol
        END AS token0_symbol,
        token0_decimals,
        CASE
            WHEN token1_symbol IS NULL THEN ''
            ELSE token1_symbol
        END AS token1_symbol,
        token1_decimals,
        CASE
            WHEN token2_symbol IS NULL THEN ''
            ELSE token2_symbol
        END AS token2_symbol,
        token2_decimals,
        CASE
            WHEN token3_symbol IS NULL THEN ''
            ELSE token3_symbol
        END AS token3_symbol,
        token3_decimals,
        CASE
            WHEN token4_symbol IS NULL THEN ''
            ELSE token4_symbol
        END AS token4_symbol,
        token4_decimals,
        CASE
            WHEN token5_symbol IS NULL THEN ''
            ELSE token5_symbol
        END AS token5_symbol,
        token5_decimals,
        CASE
            WHEN token6_symbol IS NULL THEN ''
            ELSE token6_symbol
        END AS token6_symbol,
        token6_decimals,
        CASE
            WHEN token7_symbol IS NULL THEN ''
            ELSE token7_symbol
        END AS token7_symbol,
        token7_decimals,
        pool_address,
        _inserted_timestamp
    FROM
        join_meta
)
SELECT
    tx_hash,
    poolId,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    token_array,
    token0_symbol,
    token0_decimals,
    token1_symbol,
    token1_decimals,
    token2_symbol,
    token2_decimals,
    token3_symbol,
    token3_decimals,
    token4_symbol,
    token4_decimals,
    token5_symbol,
    token5_decimals,
    token6_symbol,
    token6_decimals,
    token7_symbol,
    token7_decimals,
    CASE
        WHEN token7 IS NOT NULL THEN CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            '-',
            token2_symbol,
            '-',
            token3_symbol,
            '-',
            token4_symbol,
            '-',
            token5_symbol,
            '-',
            token6_symbol,
            '-',
            token7_symbol,
            ' BLP'
        )
        WHEN token6 IS NOT NULL THEN CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            '-',
            token2_symbol,
            '-',
            token3_symbol,
            '-',
            token4_symbol,
            '-',
            token5_symbol,
            '-',
            token6_symbol,
            ' BLP'
        )
        WHEN token5 IS NOT NULL THEN CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            '-',
            token2_symbol,
            '-',
            token3_symbol,
            '-',
            token4_symbol,
            '-',
            token5_symbol,
            ' BLP'
        )
        WHEN token4 IS NOT NULL THEN CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            '-',
            token2_symbol,
            '-',
            token3_symbol,
            '-',
            token4_symbol,
            ' BLP'
        )
        WHEN token3 IS NOT NULL THEN CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            '-',
            token2_symbol,
            '-',
            token3_symbol,
            ' BLP'
        )
        WHEN token2 IS NOT NULL THEN CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            '-',
            token2_symbol,
            ' BLP'
        )
        ELSE CONCAT(
            token0_symbol,
            '-',
            token1_symbol,
            ' BLP'
        )
    END AS pool_name,
    pool_address,
    _inserted_timestamp
FROM
    FINAL
