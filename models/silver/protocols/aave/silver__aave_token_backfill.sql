{{ config(
    materialized = 'incremental',
    unique_key = "atoken_address",
    tags = ['curated']
) }}


with contracts as (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
aave_token_pull AS (

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
            LEFT JOIN contracts C
            ON a_token_address = C.address
            LEFT JOIN contracts c2
            ON underlying_address = c2.address
        WHERE
            topics [0] = '0x1d9fcd0dc935b4778d5af97f55c4d7b2553257382f1ef25c412114c8eeebd88e'
            AND (
                a_token_name LIKE '%Aave%'
                OR a_token_name = 'Gho Token'
            )

    {% if is_incremental() %}
    AND l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
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
        LEFT JOIN contracts C
        ON a_token_address = C.address
        LEFT JOIN contracts c2
        ON underlying_address = c2.address
    WHERE
        topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
        AND block_number < 16291127
        AND (
            a_token_name LIKE '%Aave%'
            OR c2.symbol = 'GHO'
        )

    {% if is_incremental() %}
    AND l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
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
),

aave_backfill_1 as (
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
),

decode AS (
   
    SELECT
        block_number AS atoken_created_block,
        contract_address as a_token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS aave_version_pool,
        utils.udf_hex_to_int(
            SUBSTR(segmented_data [2] :: STRING, 27, 40)) :: INTEGER AS atoken_decimals,
        utils.udf_hex_to_string  
        (segmented_data [7] :: STRING) :: STRING AS atoken_name,
        utils.udf_hex_to_string  
        (segmented_data [9] :: STRING) :: STRING AS atoken_symbol,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] = '0xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    {% endif %}
),
a_token_step_1 AS (
    select 
        * 
    from 
        decode 
    where 
        atoken_name like '%Aave%'
),
debt_tokens as (
  SELECT
      block_number AS atoken_created_block,
      contract_address AS a_token_address,
      regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
      CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
      CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS atoken_address,
      CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS atoken_stable_debt_address,
      CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS atoken_variable_debt_address,
      _inserted_timestamp,
      _log_id
  FROM
      {{ ref('silver__logs') }}
  WHERE
      topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
  AND
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (select a_token_address from a_token_step_1)
),
a_token_step_2 AS (
    SELECT
        *,
        CASE
            WHEN aave_version_pool = LOWER('0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2') THEN 'Aave V3'
            WHEN aave_version_pool = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN aave_version_pool = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN aave_version_pool = LOWER('0xc9f5dd4a55e4b3cb66697c6b7d7eab42923fb00f') THEN 'Aave AMM'
            ELSE 'ERROR'
        END AS protocol
    FROM
        a_token_step_1
)
SELECT
    A.atoken_created_block,
    A.atoken_symbol AS atoken_symbol,
    A.a_token_address AS atoken_address,
    B.atoken_stable_debt_address,
    B.atoken_variable_debt_address,
    A.atoken_decimals AS atoken_decimals,
    A.protocol AS atoken_version,
    atoken_name AS atoken_name,
    C.symbol AS underlying_symbol,
    A.underlying_asset AS underlying_address,
    C.decimals AS underlying_decimals,
    C.name AS underlying_name,
    A._inserted_timestamp,
    A._log_id
FROM
    a_token_step_2 A
    INNER JOIN debt_tokens b
    ON A.a_token_address = b.atoken_address
    LEFT JOIN contracts C
    ON address = A.underlying_asset
WHERE
    A.protocol <> 'ERROR'
UNION ALL
SELECT
    atoken_created_block,
    atoken_symbol,
    atoken_address,
    atoken_stable_debt_address,
    atoken_variable_debt_address,
    atoken_decimals,
    atoken_version,
    atoken_name,
    underlying_symbol,
    underlying_address,
    underlying_decimals,
    underlying_name,
    _inserted_timestamp,
    _log_id
FROM
    aave_backfill_1