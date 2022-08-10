{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['balances']
) }}

WITH prices AS (

    SELECT
        HOUR :: DATE AS prices_date,
        token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    GROUP BY
        1,
        2
),
eth_prices AS (
    SELECT
        prices_date,
        price AS eth_price
    FROM
        prices
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
token_metadata AS (
    SELECT
        LOWER(address) AS token_address_m,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
),
token_balances AS (
    SELECT
        block_date AS balance_date,
        address,
        contract_address,
        symbol,
        NAME,
        A.balance AS non_adjusted_balance,
        CASE
            WHEN decimals IS NOT NULL THEN A.balance / pow(
                10,
                decimals
            )
        END AS balance,
        CASE
            WHEN decimals IS NOT NULL THEN ROUND(
                (
                    A.balance / pow(
                        10,
                        decimals
                    )
                ) * price,
                2
            )
        END AS balance_usd,
        CASE
            WHEN decimals IS NULL THEN FALSE
            ELSE TRUE
        END AS has_decimal,
        CASE
            WHEN price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price,
        daily_activity
    FROM
        {{ ref('silver__daily_token_balances') }} A
        LEFT JOIN prices p
        ON prices_date = block_date
        AND token_address = contract_address
        LEFT JOIN token_metadata
        ON token_address_m = contract_address
),
eth_balances AS (
    SELECT
        block_date AS balance_date,
        address,
        NULL AS contract_address,
        'ETH' AS symbol,
        'Native Ether' AS NAME,
        A.balance AS non_adjusted_balance,
        A.balance / pow(
            10,
            18
        ) AS balance,
        ROUND(
            (
                A.balance / pow(
                    10,
                    18
                )
            ) * eth_price,
            2
        ) AS balance_usd,
        TRUE AS has_decimal,
        CASE
            WHEN eth_price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price,
        daily_activity
    FROM
        {{ ref('silver__daily_eth_balances') }} A
        LEFT JOIN eth_prices p
        ON prices_date = block_date
)
SELECT
    balance_date,
    address AS user_address,
    contract_address,
    symbol,
    NAME,
    non_adjusted_balance,
    balance,
    balance_usd,
    has_decimal,
    has_price,
    daily_activity
FROM
    token_balances
UNION
SELECT
    balance_date,
    address AS user_address,
    contract_address,
    symbol,
    NAME,
    non_adjusted_balance,
    balance,
    balance_usd,
    has_decimal,
    has_price,
    daily_activity
FROM
    eth_balances
