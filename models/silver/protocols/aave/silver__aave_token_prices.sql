{{ config(
    materialized = 'incremental',
    unique_key = "price_id",
    cluster_by = ['prices_hour::DATE'],
    tags = ['curated']
) }}

WITH atoken_meta AS (

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
        atoken_variable_debt_address
    FROM
        {{ ref('silver__aave_tokens') }}
),
ORACLE AS(
    SELECT
        block_hour,
        token_address,
        AVG(value_ethereum) AS value_ethereum,
        AVG(price) AS oracle_price
    FROM
        {{ ref('silver__aave_oracle_prices') }}

{% if is_incremental() %}
WHERE
    block_hour :: DATE >= CURRENT_DATE - 2
{% endif %}
GROUP BY
    1,
    2
),
backup_prices AS(
    SELECT
        HOUR,
        token_address,
        AVG(price) AS price
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT atoken_address
                FROM
                    atoken_meta
            )
            OR token_address IN (
                SELECT
                    DISTINCT underlying_address
                FROM
                    atoken_meta
            )
            OR token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        )
        AND token_address <> '0x1985365e9f78359a9b6ad760e32412f4a445e862'

{% if is_incremental() %}
AND HOUR :: DATE >= CURRENT_DATE - 2
{% endif %}
GROUP BY
    1,
    2
),
date_expand AS (
    SELECT
        DISTINCT HOUR
    FROM
        backup_prices
),
addresses AS (
    SELECT
        DISTINCT underlying_address
    FROM
        atoken_meta
),
address_list AS (
    SELECT
        *
    FROM
        date_expand
        JOIN addresses
),
eth_prices AS (
    SELECT
        HOUR AS eth_price_hour,
        price AS eth_price
    FROM
        backup_prices
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
prices_join AS (
    SELECT
        al.hour AS prices_hour,
        al.underlying_address,
        atoken_address,
        atoken_version,
        eth_price,
        oracle_price,
        bp.price AS backup_price,
        underlying_decimals,
        underlying_symbol,
        value_ethereum,
        COALESCE(
            oracle_price,
            backup_price
        ) AS hourly_price,
        concat_ws(
            '-',
            al.hour,
            al.underlying_address
        ) AS price_id
    FROM
        address_list al
        LEFT JOIN ORACLE
        ON al.underlying_address = ORACLE.token_address
        AND al.hour = ORACLE.block_hour
        LEFT JOIN atoken_meta am
        ON al.underlying_address = am.underlying_address
        LEFT JOIN backup_prices bp
        ON al.underlying_address = bp.token_address
        AND al.hour = bp.hour
        LEFT JOIN eth_prices
        ON eth_price_hour = al.hour
)
SELECT
    prices_hour,
    underlying_address,
    atoken_address,
    atoken_version,
    eth_price,
    oracle_price,
    backup_price,
    underlying_decimals,
    underlying_symbol,
    value_ethereum,
    hourly_price,
    price_id
FROM
    prices_join
WHERE
    hourly_price IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY price_id
ORDER BY
    hourly_price DESC)) = 1
