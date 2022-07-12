{{ config(
    materialized = 'view'
) }}

WITH atoken_seed AS (

    SELECT
        LOWER(atoken_address) AS atoken_address,
        atoken_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ source(
            'eth_dev_db',
            'aave_atokens_upload'
        ) }}
),
atoken_contract_base AS (
    SELECT
        LOWER(address) AS atoken_address,
        symbol AS atoken_symbol,
        NAME AS atoken_name,
        decimals AS atoken_decimals,
        LOWER(
            COALESCE(
                contract_metadata :underlyingAssetAddress :: STRING,
                contract_metadata :UNDERLYING_ASSET_ADDRESS :: STRING
            )
        ) AS underlying_address1,
        CASE
            WHEN underlying_address1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
            ELSE underlying_address1
        END AS underlying_address,
        contract_metadata AS atoken_metadata
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        address IN (
            SELECT
                atoken_address
            FROM
                atoken_seed
        )
)
SELECT
    A.atoken_address,
    A.atoken_symbol,
    A.atoken_name,
    A.atoken_decimals,
    A.atoken_metadata,
    A.underlying_address,
    CASE
        WHEN A.underlying_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' THEN 'MKR'
        WHEN A.underlying_address = '0x1985365e9f78359a9b6ad760e32412f4a445e862' THEN 'REP'
        ELSE u.symbol
    END AS underlying_symbol,
    CASE
        WHEN A.underlying_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' THEN 'Maker'
        WHEN A.underlying_address = '0x1985365e9f78359a9b6ad760e32412f4a445e862' THEN 'Reputation'
        ELSE u.name
    END AS underlying_name,
    u.decimals AS underlying_decimals,
    u.contract_metadata AS underlying_metadata,
    s.atoken_version,
    s.atoken_created_block,
    s.atoken_stable_debt_address,
    s.atoken_variable_debt_address
FROM
    atoken_contract_base A
    LEFT JOIN {{ ref('core__dim_contracts') }}
    u
    ON underlying_address = u.address
    LEFT JOIN atoken_seed s
    ON A.atoken_address = s.atoken_address
