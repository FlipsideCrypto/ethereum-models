{{ config(
    materialized = 'incremental',
    unique_key = "atoken_address",
    tags = ['non_realtime']
) }}

WITH debt_tokens_1 AS (
    SELECT
        contract_address AS debt_token_address,
        decoded_flat: debtTokenName :: STRING AS debt_token_name,
        decoded_flat: debtTokenDecimals :: STRING,
        decoded_flat: debtTokenSymbol :: STRING,
        decoded_flat: pool :: STRING AS spark_version_pool,
        decoded_flat: underlyingAsset :: STRING AS underlying_asset
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] = '0x40251fbfb6656cfa65a00d7879029fec1fad21d28fdcff2f4f68f52795b74f2c'
        AND decoded_flat: debtTokenName :: STRING LIKE '%Spark%'
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
            WHEN spark_version_pool = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987') THEN 'Spark'
            ELSE 'ERROR'
        END AS protocol
    FROM
        debt_tokens_1
    WHERE
        protocol <> 'ERROR'
),
debt_tokens_3 AS (
    SELECT
        underlying_asset,
        protocol,
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
        decoded_flat: pool :: STRING AS spark_version_pool,
        decoded_flat: treasury :: STRING AS treasury,
        decoded_flat: underlyingAsset :: STRING AS underlying_asset,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] = '0xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'
        AND decoded_flat: aTokenName :: STRING LIKE '%Spark%'

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
            WHEN spark_version_pool = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987') THEN 'Spark'
            ELSE 'ERROR'
        END AS protocol
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
    A.protocol AS atoken_version,
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
    AND A.protocol = b.protocol
    LEFT JOIN ethereum_dev.silver.contracts C
    ON address = A.underlying_asset
WHERE
    A.protocol <> 'ERROR'
