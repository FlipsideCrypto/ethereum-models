{{ config(
    materialized = 'table',
    unique_key = 'nft_address',
    tags = ['nft_api']
) }}

WITH top_collection AS (

    SELECT
        nft_address,
        SUM(price) AS total_price
    FROM
        {{ ref('core__ez_nft_sales') }}
    WHERE
        currency_symbol IN (
            'ETH',
            'WETH'
        )
    GROUP BY
        nft_address qualify ROW_NUMBER() over (
            ORDER BY
                total_price DESC
        ) <= 500
),
recent_collection AS (
    SELECT
        nft_address,
        SUM(price) AS total_price
    FROM
        {{ ref('core__ez_nft_sales') }}
    WHERE
        block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hour'
        AND currency_symbol IN (
            'ETH',
            'WETH'
        )
        AND nft_address NOT IN (
            SELECT
                nft_address
            FROM
                top_collection
        )
    GROUP BY
        nft_address qualify ROW_NUMBER() over (
            ORDER BY
                total_price DESC
        ) <= 20
),
all_collection AS (
    SELECT
        nft_address
    FROM
        top_collection
    UNION
    SELECT
        nft_address
    FROM
        recent_collection
)
SELECT
    *
FROM
    all_collection
