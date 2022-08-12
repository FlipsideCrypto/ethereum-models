{{ config(
    materialized = 'incremental',
    unique_key = "token_contract",
    tags = ['snowflake', 'ethereum', 'aave', 'aave_tokens']
) }}

WITH atokens AS(

    SELECT
        inputs :_reserve :: STRING AS reserve_token,
        A.value :: STRING AS balances,
        CASE
            WHEN contract_address IN(
                LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
                LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')
            ) THEN 'Aave V2'
            WHEN contract_address IN(
                LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
                LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
            ) THEN 'Aave AMM'
            ELSE 'Aave V1'
        END AS aave_version
    FROM
        {{ source(
            'flipside_silver_ethereum',
            'reads'
        ) }},
        LATERAL FLATTEN(input => SPLIT(value_string, '^')) A
    WHERE
        1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= CURRENT_DATE - 2
{% else %}
    AND block_timestamp :: DATE >= CURRENT_DATE - 720
{% endif %}
AND contract_address IN (
    LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),
    -- AAVE V2 Data Provider (per docs)
    LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
    -- AAVE AMM Lending Pool (per docs)
    LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
    -- AAVE V2 Lending Pool (per docs)
    LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),
    -- AAVE AMM Data Provider (per docs)
    LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119')
) -- AAVE V1
),
FINAL AS (
    SELECT
        CASE
            WHEN LOWER(reserve_token) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
            ELSE LOWER(reserve_token)
        END AS token_contract,
        aave_version,
        MAX(
            CASE
                WHEN SPLIT(
                    balances,
                    ':'
                ) [0] :: STRING = 'aTokenAddress' THEN SPLIT(
                    balances,
                    ':'
                ) [1]
                ELSE ''
            END
        ) AS aave_token1,
        LOWER(aave_token1) AS aave_token
    FROM
        atokens
    GROUP BY
        1,
        2
),
contracts AS (
    SELECT
        LOWER(address) AS token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        1 = 1
        AND decimals IS NOT NULL
)
SELECT
    token_contract,
    aave_version,
    aave_token,
    CASE
        WHEN token_contract = '0x1985365e9f78359a9b6ad760e32412f4a445e862' THEN 'REP'
        WHEN token_contract = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' THEN 'MKR'
        WHEN token_contract = '0x50379f632ca68d36e50cfbc8f78fe16bd1499d1e' THEN 'G-UNI'
        WHEN token_contract = '0xd2eec91055f07fe24c9ccb25828ecfefd4be0c41' THEN 'G-UNI'
        WHEN token_contract = '0xa693b19d2931d498c5b318df961919bb4aee87a5' THEN 'UST'
        ELSE A.symbol
    END AS token_symbol,
    COALESCE(
        A.decimals,
        18
    ) AS token_decimals,
    CONCAT(
        'a',
        token_symbol
    ) AS atoken_symbols,
    token_decimals AS atoken_decimals,
    b.symbol AS s_test,
    b.decimals AS d_test
FROM
    FINAL
    LEFT JOIN contracts A
    ON A.token_address = token_contract
    LEFT JOIN contracts b
    ON b.token_address = aave_token
WHERE
    token_contract IS NOT NULL
