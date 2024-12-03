{{ config(
    materialized = 'table',
    unique_key = 'nft_address'
) }}

SELECT
    '0x790b2cf29ed4f310bf7641f013c65d4560d28371' AS nft_address,
    55142 AS mint_count,
    1 AS start_page,
    CEIL(
        mint_count / 100
    ) AS end_page
LIMIT
    1
