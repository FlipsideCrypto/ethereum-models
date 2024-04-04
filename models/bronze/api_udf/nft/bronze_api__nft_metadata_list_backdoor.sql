{{ config(
    materialized = 'table',
    unique_key = 'nft_address'
) }}

SELECT
    '0x23581767a106ae21c074b2276D25e5C3e136a68b' AS nft_address,
    10000 AS mint_count,
    1 AS start_page,
    CEIL(
        mint_count / 100
    ) AS end_page
LIMIT
    1
