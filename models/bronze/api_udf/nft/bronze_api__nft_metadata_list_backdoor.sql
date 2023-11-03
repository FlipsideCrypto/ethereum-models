{{ config(
    materialized = 'table'
) }}

SELECT
    '0x4fdf87d4edae3fe323b8f6df502ccac6c8b4ba28' AS nft_address
LIMIT
    1
