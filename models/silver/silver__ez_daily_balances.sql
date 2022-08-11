{{ config(
    materialized = 'view',
    tags = ['balances'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH prices AS (

    SELECT
        HOUR :: DATE AS price_date,
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
daily_tokens AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        address,
        contract_address,
        AVG(
            current_bal_unadj :: FLOAT
        ) AS avg_daily_bal_unadj
    FROM
        {{ ref("silver__token_balance_diffs") }}
    WHERE
        block_timestamp :: DATE >= '2020-01-01'
    GROUP BY
        1,
        2,
        3
),
daily_eth AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        address,
        NULL AS contract_address,
        AVG(
            current_bal_unadj :: FLOAT
        ) AS avg_daily_bal_unadj
    FROM
        {{ ref("silver__eth_balance_diffs") }}
    WHERE
        block_timestamp :: DATE >= '2020-01-01'
    GROUP BY
        1,
        2,
        3
),
all_daily_bals AS (
    SELECT
        block_date,
        address,
        contract_address,
        avg_daily_bal_unadj
    FROM
        daily_tokens
    UNION ALL
    SELECT
        block_date,
        address,
        contract_address,
        avg_daily_bal_unadj
    FROM
        daily_eth
),
block_dates AS (
    SELECT
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
),
address_ranges AS (
    SELECT
        address,
        contract_address,
        CURRENT_DATE() :: DATE AS max_block_date,
        MIN(
            block_date :: DATE
        ) AS min_block_date
    FROM
        all_daily_bals
    GROUP BY
        1,
        2,
        3
),
all_dates AS (
    SELECT
        C.block_date,
        A.address,
        A.contract_address
    FROM
        block_dates C
        LEFT JOIN address_ranges A
        ON C.block_date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.address IS NOT NULL
),
eth_balances AS (
    SELECT
        address,
        contract_address,
        block_date,
        avg_daily_bal_unadj,
        TRUE AS daily_activity
    FROM
        all_daily_bals
),
balance_tmp AS (
    SELECT
        d.block_date,
        d.address,
        d.contract_address,
        b.avg_daily_bal_unadj :: FLOAT AS balance,
        b.daily_activity
    FROM
        all_dates d
        LEFT JOIN eth_balances b
        ON d.block_date = b.block_date
        AND d.address = b.address
        AND d.contract_address = b.contract_address
),
final_carry AS (
    SELECT
        block_date,
        address,
        contract_address,
        LAST_VALUE(
            balance ignore nulls
        ) over(
            PARTITION BY address,
            contract_address
            ORDER BY
                block_date ASC rows unbounded preceding
        ) AS balance_unadj,
        CASE
            WHEN daily_activity IS NULL THEN FALSE
            ELSE TRUE
        END AS daily_activity
    FROM
        balance_tmp
),
FINAL AS (
    SELECT
        block_date,
        address,
        contract_address,
        balance_unadj,
        CASE
            WHEN contract_address IS NULL THEN 'ETH'
            ELSE symbol
        END AS symbol_n,
        CASE
            WHEN contract_address IS NULL THEN 'Native Ether'
            ELSE NAME
        END AS name_n,
        CASE
            WHEN contract_address IS NULL THEN 18
            ELSE decimals
        END AS decimals_n,
        daily_activity,
        CASE
            WHEN decimals_n IS NOT NULL THEN balance_unadj / pow(
                10,
                decimals_n
            )
        END AS balance_adj,
        CASE
            WHEN decimals_n IS NOT NULL THEN balance_adj * price
        END AS balance_adj_usd,
        CASE
            WHEN decimals_n IS NULL THEN FALSE
            ELSE TRUE
        END AS has_decimal,
        CASE
            WHEN price IS NULL THEN FALSE
            ELSE TRUE
        END AS has_price
    FROM
        final_carry A
        LEFT JOIN token_metadata b
        ON A.contract_address = b.token_address
        LEFT JOIN prices p
        ON p.price_date :: DATE = A.block_date :: DATE
        AND (
            CASE
                WHEN contract_address IS NULL THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE contract_address
            END
        ) = p.token_address
    WHERE
        balance_unadj <> 0
)
SELECT
    block_date AS balance_date,
    address AS user_address,
    contract_address,
    balance_unadj,
    balance_adj AS balance,
    balance_adj_usd AS balance_usd,
    symbol_n AS symbol,
    name_n AS NAME,
    decimals_n AS decimals,
    daily_activity,
    has_decimal,
    has_price
FROM
    FINAL
