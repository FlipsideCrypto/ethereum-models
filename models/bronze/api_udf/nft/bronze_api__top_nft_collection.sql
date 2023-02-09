{{ config(
    materialized = 'incremental',
    unique_key = 'nft_address'
) }}

WITH base 


WITH top_collection as (
    SELECT
        nft_address,
        SUM(price) as total_price
    FROM
        {{ ref('core__ez_nft_sales')}}
    WHERE 
        currency_symbol IN ('ETH', 'WETH')
    AND 
        platform_name IN ('opensea')

    GROUP BY
        nft_address

    QUALIFY ROW_NUMBER() OVER (ORDER BY total_price DESC) <= 500
), 

recent_collection as (
    SELECT
        nft_address,
        SUM(price) as total_price
    FROM
        {{ ref('core__ez_nft_sales')}}
    WHERE 
        block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hour'
    AND
        currency_symbol IN ('ETH', 'WETH')
    AND 
        platform_name IN ('opensea')
    
    AND nft_address NOT IN (
        SELECT 
            nft_address
        FROM 
            top_collection
    )

    GROUP BY
        nft_address

    QUALIFY ROW_NUMBER() OVER (ORDER BY total_price DESC) <= 10   
)
