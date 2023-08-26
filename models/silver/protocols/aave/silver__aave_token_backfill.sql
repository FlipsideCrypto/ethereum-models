{{ config(
    materialized = 'incremental',
    unique_key = "atoken_address",
    tags = ['non_realtime']
) }}

WITH aave_v1_1 AS (

    SELECT
        block_number AS atoken_created_block,
        C.symbol AS a_token_symbol,
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
        ethereum_dev.silver.logs l
        LEFT JOIN ethereum_dev.silver.contracts C
        ON a_token_address = C.address
        LEFT JOIN ethereum_dev.silver.contracts c2
        ON underlying_address = c2.address
    WHERE
        topics [0] = '0x1d9fcd0dc935b4778d5af97f55c4d7b2553257382f1ef25c412114c8eeebd88e'
        AND origin_from_address = LOWER('0x2fbB0c60a41cB7Ea5323071624dCEAD3d213D0Fa')
        AND a_token_name IS NOT NULL

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
aave_v1_2 AS (
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
        underlying_address,
        _inserted_timestamp,
        _log_id
    FROM
        aave_v1_1
),
debt_tokens_1 AS (
    SELECT
        contract_address AS debt_token_address,
        decoded_flat: debtTokenName :: STRING AS debt_token_name,
        decoded_flat: debtTokenDecimals :: STRING,
        decoded_flat: debtTokenSymbol :: STRING,
        decoded_flat: pool :: STRING AS aave_version_pool,
        decoded_flat: underlyingAsset :: STRING AS underlying_asset
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] = '0x40251fbfb6656cfa65a00d7879029fec1fad21d28fdcff2f4f68f52795b74f2c'
        AND decoded_flat: debtTokenName :: STRING LIKE '%Aave%'
),
debt_tokens_2 AS (
    SELECT
        debt_token_address,
        debt_token_name,
        underlying_asset,
        CASE
            WHEN debt_token_name LIKE '%variable%' THEN 'variable_debt_token'
            WHEN debt_token_name LIKE '%Variable%' THEN 'variable_debt_token'
            WHEN debt_token_name LIKE '%stable%' THEN 'stable_debt_token'
            WHEN debt_token_name LIKE '%Stable%' THEN 'stable_debt_token'
            ELSE 'Other'
        END AS token_type,
        CASE
            WHEN debt_token_name LIKE '%variable%' THEN debt_token_address
            WHEN debt_token_name LIKE '%Variable%' THEN debt_token_address
        END AS variable_debt_address,
        CASE
            WHEN debt_token_name LIKE '%stable%' THEN debt_token_address
            WHEN debt_token_name LIKE '%Stable%' THEN debt_token_address
        END AS stable_debt_address,
        CASE
            WHEN aave_version_pool = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN aave_version_pool = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN aave_version_pool = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN aave_version_pool = LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version
    FROM
        debt_tokens_1
    WHERE
        aave_version <> 'ERROR'
),
debt_tokens_3 AS (
    SELECT
        underlying_asset,
        aave_version,
        MAX(variable_debt_address) AS atoken_variable_debt_address,
        MAX(stable_debt_address) AS atoken_stable_debt_address
    FROM
        debt_tokens_2
    GROUP BY
        ALL
),
a_token_step_1 AS (
    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
        decoded_flat: aTokenName :: STRING AS a_token_name,
        decoded_flat: aTokenDecimals :: STRING AS a_token_decimals,
        decoded_flat: aTokenSymbol :: STRING AS a_token_symbol,
        decoded_flat: pool :: STRING AS aave_version_pool,
        decoded_flat: treasury :: STRING AS treasury,
        decoded_flat: underlyingAsset :: STRING AS underlying_asset,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] = '0xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'
        AND decoded_flat: aTokenName :: STRING LIKE '%Aave%'

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
a_token_step_2 AS (
    SELECT
        *,
        CASE
            WHEN aave_version_pool = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN aave_version_pool = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN aave_version_pool = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN aave_version_pool = LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version
    FROM
        a_token_step_1
)
SELECT
    atoken_created_block,
    a_token_symbol AS atoken_symbol,
    a_token_address AS atoken_address,
    atoken_stable_debt_address,
    atoken_variable_debt_address,
    a_token_decimals AS atoken_decimals,
    A.aave_version AS atoken_version,
    a_token_name AS atoken_name,
    C.symbol AS underlying_symbol,
    A.underlying_asset AS underlying_address,
    C.decimals AS underlying_decimals,
    C.name AS underlying_name,
    A._inserted_timestamp,
    A._log_id
FROM
    a_token_step_2 A
    LEFT JOIN debt_tokens_3 b
    ON A.underlying_asset = b.underlying_asset
    AND A.aave_version = b.aave_version
    LEFT JOIN ethereum_dev.silver.contracts C
    ON address = A.underlying_asset
WHERE
    A.aave_version <> 'ERROR'
UNION ALL
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
    aave_v1_2
