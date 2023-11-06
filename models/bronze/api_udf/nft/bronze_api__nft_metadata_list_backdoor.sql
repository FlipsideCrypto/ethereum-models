{{ config(
    materialized = 'table'
) }}

SELECT
    '0x4fdf87d4edae3fe323b8f6df502ccac6c8b4ba28' AS nft_address,
    233 AS mint_count,
    1 AS start_page,
    CEIL(
        mint_count / 100
    ) AS end_page
LIMIT
    1
