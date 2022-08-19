{{ config(
    materialized = 'view',
    tags = ['balances'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH prices AS (

    SELECT
        HOUR,
        LOWER(token_address) AS token_address,
        AVG(price) AS price
    FROM
        {{ ref("core__fact_hourly_token_prices") }}
    WHERE
        price IS NOT NULL
        AND token_address IS NOT NULL
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
        {{ ref("core__dim_contracts") }}
),
last_price AS (
    SELECT
        HOUR,
        token_address,
        price
    FROM
        prices qualify(ROW_NUMBER() over (PARTITION BY token_address
    ORDER BY
        HOUR DESC)) = 1
),
latest_tokens AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        current_bal_unadj
    FROM
        {{ ref("silver__token_balance_diffs") }}
        qualify(ROW_NUMBER() over (PARTITION BY address, contract_address
    ORDER BY
        block_number DESC)) = 1
),
latest_eth AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        NULL AS contract_address,
        current_bal_unadj
    FROM
        {{ ref("silver__eth_balance_diffs") }}
        qualify(ROW_NUMBER() over (PARTITION BY address
    ORDER BY
        block_number DESC)) = 1
),
token_diffs AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        current_bal_unadj,
        CASE
            WHEN decimals IS NOT NULL THEN current_bal_unadj / pow(
                10,
                decimals
            )
        END AS current_bal,
        CASE
            WHEN decimals IS NOT NULL THEN ROUND(
                current_bal * A.price,
                2
            )
        END AS usd_value_last_activity,
        CASE
            WHEN decimals IS NOT NULL THEN ROUND(
                current_bal * b.price,
                2
            )
        END AS usd_value_now,
        symbol,
        NAME,
        decimals,
        CASE
            WHEN decimals IS NULL THEN FALSE
            ELSE TRUE
        END AS has_decimal,
        CASE
            WHEN A.price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price,
        b.hour :: TIMESTAMP AS last_recorded_price
    FROM
        latest_tokens base
        LEFT JOIN token_metadata
        ON base.contract_address = token_metadata.token_address
        LEFT JOIN prices A
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = A.hour
        AND A.token_address = base.contract_address
        LEFT JOIN last_price b
        ON b.token_address = base.contract_address
    WHERE
        current_bal_unadj <> 0
),
eth_diffs AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        current_bal_unadj,
        current_bal_unadj / pow(
            10,
            18
        ) AS current_bal,
        ROUND(
            current_bal * A.price,
            2
        ) AS usd_value_last_activity,
        ROUND(
            current_bal * b.price,
            2
        ) AS usd_value_now,
        'ETH' AS symbol,
        'Native Ether' AS NAME,
        18 AS decimals,
        TRUE AS has_decimal,
        CASE
            WHEN A.price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price,
        b.hour :: TIMESTAMP AS last_recorded_price
    FROM
        latest_eth
        LEFT JOIN prices A
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = A.hour
        AND A.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        LEFT JOIN last_price b
        ON b.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    WHERE
        current_bal_unadj <> 0
)
SELECT
    block_number AS last_activity_block,
    block_timestamp AS last_activity_block_timestamp,
    address AS user_address,
    contract_address,
    current_bal_unadj,
    current_bal,
    usd_value_last_activity,
    usd_value_now,
    symbol,
    NAME AS token_name,
    decimals,
    has_decimal,
    has_price,
    last_recorded_price
FROM
    eth_diffs
UNION ALL
SELECT
    block_number AS last_activity_block,
    block_timestamp AS last_activity_block_timestamp,
    address AS user_address,
    contract_address,
    current_bal_unadj,
    current_bal,
    usd_value_last_activity,
    usd_value_now,
    symbol,
    NAME AS token_name,
    decimals,
    has_decimal,
    has_price,
    last_recorded_price
FROM
    token_diffs
