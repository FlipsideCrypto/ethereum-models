{{ config(
    materialized = 'table',
    unique_key = 'nft_address'
) }}

WITH top_collection AS (
    SELECT
        nft_address,
        SUM(price) as total_price
    FROM
        {{ ref('core__ez_nft_sales')}}
    WHERE 
        currency_symbol IN ('ETH', 'WETH')

    GROUP BY
        nft_address

    QUALIFY ROW_NUMBER() OVER (ORDER BY total_price DESC) <= 500
), 

recent_collection AS (
    SELECT
        nft_address,
        SUM(price) as total_price
    FROM
        {{ ref('core__ez_nft_sales')}}
    WHERE 
        block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hour'
    AND
        currency_symbol IN ('ETH', 'WETH')
    
    AND nft_address NOT IN (
        SELECT 
            nft_address
        FROM 
            top_collection
    )

    GROUP BY
        nft_address

    QUALIFY ROW_NUMBER() OVER (ORDER BY total_price DESC) <= 10   
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
