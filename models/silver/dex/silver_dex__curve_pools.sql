{{ config(
    materialized = 'incremental',
    unique_key = "pool_address"
) }}

WITH pool_tokens AS (

    SELECT
        DISTINCT LOWER(
            inputs :_pool :: STRING
        ) AS pool_add,
        (SPLIT(LOWER(value_string), '^')) AS coins
    FROM
        {{ source(
            'flipside_silver_ethereum',
            'reads'
        ) }}
    WHERE
        contract_name = 'Vyper_contract'
        AND contract_address IN (
            '0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
            LOWER('0xfD6f33A0509ec67dEFc500755322aBd9Df1bD5B8'),
            '0x90e00ace148ca3b23ac1bc8c240c2a7dd9c2d7f5',
            '0x7D86446dDb609eD0F5f8684AcF30380a356b2B4c'
        )
        AND function_name = 'get_underlying_coins'
        AND block_timestamp >= CURRENT_DATE - 60
),
pool_tokens_parsed AS (
    SELECT
        pool_add,
        CASE
            WHEN VALUE :: STRING = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE VALUE :: STRING
        END AS coins,
        (ROW_NUMBER() over (PARTITION BY pool_add
    ORDER BY
        pool_add DESC) - 1) AS INDEX
    FROM
        pool_tokens,
        TABLE(FLATTEN(pool_tokens.coins))
    WHERE
        VALUE :: STRING <> '0x0000000000000000000000000000000000000000'
)
SELECT
    pool_add AS pool_address,
    coins
FROM
    pool_tokens_parsed
