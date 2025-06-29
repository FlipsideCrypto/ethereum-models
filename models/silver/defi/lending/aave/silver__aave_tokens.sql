{{ config(
    materialized = 'view',
    tags = ['silver','defi','lending','curated']
) }}

WITH base AS (

    SELECT
        LOWER(atoken_address) AS atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        LOWER(underlying_address) AS underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        LOWER(atoken_stable_debt_address) AS atoken_stable_debt_address,
        LOWER(atoken_variable_debt_address) AS atoken_variable_debt_address
    FROM
        {{ ref('silver__aave_token_backfill') }}
)
SELECT
    atoken_address,
    atoken_symbol,
    atoken_name,
    atoken_decimals,
    underlying_address,
    underlying_symbol,
    underlying_name,
    underlying_decimals,
    atoken_version,
    atoken_created_block,
    atoken_stable_debt_address,
    atoken_variable_debt_address,
    NULL AS atoken_metadata,
    NULL AS underlying_metadata
FROM
    base
    LEFT JOIN {{ ref('core__dim_contracts') }}
    c1
    ON LOWER(
        c1.address
    ) = atoken_address
    LEFT JOIN {{ ref('core__dim_contracts') }}
    c2
    ON LOWER(
        c2.address
    ) = underlying_address qualify(ROW_NUMBER() over(PARTITION BY atoken_address
ORDER BY
    atoken_created_block DESC)) = 1
