{{ config(
    materialized = 'incremental',
    unique_key = "atoken_address",
    tags = ['non_realtime']
) }}

WITH aave_token_pull AS (

    SELECT
        block_number AS atoken_created_block,
        C.symbol AS a_token_symbol,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS a_token_address,
        NULL AS atoken_stable_debt_address,
        NULL AS atoken_variable_debt_address,
        C.decimals AS a_token_decimals,
        'Aave V1' AS aave_version,
        C.name AS a_token_name,
        c2.symbol AS underlying_symbol,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_address,
        c2.name AS underlying_name,
        c2.decimals AS underlying_decimals,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN ethereum_dev.silver.contracts C
        ON a_token_address = C.address
        LEFT JOIN ethereum_dev.silver.contracts c2
        ON underlying_address = c2.address
    WHERE
        topics [0] = '0x1d9fcd0dc935b4778d5af97f55c4d7b2553257382f1ef25c412114c8eeebd88e'
        AND (
            a_token_name LIKE '%Aave%'
            OR a_token_name = 'Gho Token'
        )

{% if is_incremental() %}
AND l._inserted_timestamp > (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    block_number AS atoken_created_block,
    C.symbol AS a_token_symbol,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS a_token_address,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS atoken_stable_debt_address,
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS atoken_variable_debt_address,
    C.decimals AS a_token_decimals,
    CASE
        WHEN C.name LIKE '%AMM%' THEN 'Aave AMM'
        ELSE 'Aave V2'
    END AS aave_version,
    C.name AS a_token_name,
    c2.symbol AS underlying_symbol,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_address,
    c2.name AS underlying_name,
    c2.decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM
    {{ ref('silver__logs') }}
    l
    LEFT JOIN ethereum_dev.silver.contracts C
    ON a_token_address = C.address
    LEFT JOIN ethereum_dev.silver.contracts c2
    ON underlying_address = c2.address
WHERE
    topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
    AND block_number < 16291127
    AND (
        a_token_name LIKE '%Aave%'
        OR c2.symbol = 'GHO'
    )

{% if is_incremental() %}
AND l._inserted_timestamp > (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    block_number AS atoken_created_block,
    C.symbol AS a_token_symbol,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS a_token_address,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS atoken_stable_debt_address,
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS atoken_variable_debt_address,
    C.decimals AS a_token_decimals,
    CASE
        WHEN C.name LIKE '%AMM%' THEN 'Aave AMM'
        ELSE 'Aave V3'
    END AS aave_version,
    C.name AS a_token_name,
    c2.symbol AS underlying_symbol,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_address,
    c2.name AS underlying_name,
    c2.decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM
    {{ ref('silver__logs') }}
    l
    LEFT JOIN ethereum_dev.silver.contracts C
    ON a_token_address = C.address
    LEFT JOIN ethereum_dev.silver.contracts c2
    ON underlying_address = c2.address
WHERE
    topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
    AND block_number >= 16291127
    AND (
        a_token_name LIKE '%Aave%'
        OR c2.symbol = 'GHO'
    )


{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE -1
    FROM
        {{ this }}
)
{% endif %}
),
aave_token_pull_2 AS (
    SELECT
        atoken_created_block,
        a_token_symbol,
        a_token_address,
        atoken_stable_debt_address,
        atoken_variable_debt_address,
        a_token_decimals,
        aave_version,
        a_token_name,
        CASE
            WHEN underlying_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH'
            ELSE underlying_symbol
        END AS underlying_symbol,
        CASE
            WHEN underlying_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'Ethereum'
            ELSE underlying_name
        END AS underlying_name,
        CASE
            WHEN underlying_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18
            ELSE underlying_decimals
        END AS underlying_decimals,
        CASE
            WHEN underlying_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE underlying_address
        END AS underlying_address,
        _inserted_timestamp,
        _log_id
    FROM
        aave_token_pull
)
SELECT
    atoken_created_block,
    a_token_symbol AS atoken_symbol,
    a_token_address AS atoken_address,
    atoken_stable_debt_address,
    atoken_variable_debt_address,
    a_token_decimals AS atoken_decimals,
    aave_version AS atoken_version,
    a_token_name AS atoken_name,
    underlying_symbol,
    underlying_address,
    underlying_decimals,
    underlying_name,
    _inserted_timestamp,
    _log_id
FROM
    aave_token_pull_2
