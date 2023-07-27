{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'BALANCES'
            }
        }
    },
    tags = ['balances','non_realtime']
) }}

WITH prices AS (

    SELECT
        HOUR,
        LOWER(token_address) AS token_address,
        AVG(price) AS price
    FROM
        {{ ref("core__fact_hourly_token_prices") }}
    GROUP BY
        1,
        2
),
token_metadata AS (
    SELECT
        LOWER(address) AS token_address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref("silver__contracts") }}
),
eth_prices AS (
    SELECT
        HOUR,
        price AS eth_price
    FROM
        prices
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
token_diffs AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        prev_bal_unadj,
        CASE
            WHEN decimals IS NOT NULL THEN prev_bal_unadj / pow(
                10,
                decimals
            )
        END AS prev_bal,
        CASE
            WHEN decimals IS NOT NULL THEN ROUND(
                prev_bal * price,
                2
            )
        END AS prev_bal_usd,
        current_bal_unadj,
        CASE
            WHEN decimals IS NOT NULL THEN current_bal_unadj / pow(
                10,
                decimals
            )
        END AS current_bal,
        CASE
            WHEN decimals IS NOT NULL THEN ROUND(
                current_bal * price,
                2
            )
        END AS current_bal_usd,
        symbol,
        NAME,
        decimals,
        CASE
            WHEN decimals IS NULL THEN FALSE
            ELSE TRUE
        END AS has_decimal,
        CASE
            WHEN price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price
    FROM
        {{ ref("silver__token_balance_diffs") }}
        base
        LEFT JOIN token_metadata
        ON base.contract_address = token_metadata.token_address
        LEFT JOIN prices
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
        AND prices.token_address = base.contract_address
),
eth_diffs AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        NULL AS contract_address,
        prev_bal_unadj,
        prev_bal_unadj / pow(
            10,
            18
        ) AS prev_bal,
        ROUND(
            prev_bal * eth_price,
            2
        ) AS prev_bal_usd,
        current_bal_unadj,
        current_bal_unadj / pow(
            10,
            18
        ) AS current_bal,
        ROUND(
            current_bal * eth_price,
            2
        ) AS current_bal_usd,
        'ETH' AS symbol,
        'Native Ether' AS NAME,
        18 AS decimals,
        TRUE AS has_decimal,
        CASE
            WHEN eth_price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price
    FROM
        {{ ref("silver__eth_balance_diffs") }}
        LEFT JOIN eth_prices
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
)
SELECT
    block_number,
    block_timestamp,
    address AS user_address,
    contract_address,
    prev_bal_unadj,
    prev_bal,
    prev_bal_usd,
    current_bal_unadj,
    current_bal,
    current_bal_usd,
    current_bal_unadj - prev_bal_unadj AS bal_delta_unadj,
    current_bal - prev_bal AS bal_delta,
    current_bal_usd - prev_bal_usd AS bal_delta_usd,
    symbol,
    NAME AS token_name,
    decimals,
    has_decimal,
    has_price
FROM
    eth_diffs
UNION ALL
SELECT
    block_number,
    block_timestamp,
    address AS user_address,
    contract_address,
    prev_bal_unadj,
    prev_bal,
    prev_bal_usd,
    current_bal_unadj,
    current_bal,
    current_bal_usd,
    current_bal_unadj - prev_bal_unadj AS bal_delta_unadj,
    current_bal - prev_bal AS bal_delta,
    current_bal_usd - prev_bal_usd AS bal_delta_usd,
    symbol,
    NAME AS token_name,
    decimals,
    has_decimal,
    has_price
FROM
    token_diffs
