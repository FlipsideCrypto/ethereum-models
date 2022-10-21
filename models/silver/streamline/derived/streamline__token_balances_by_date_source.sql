{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT 
    CONTRACT_ADDRESS,
    BLOCK_NUMBER,
    '0x70a08231000000000000000000000000' || LTRIM(ADDRESS, 2) as ADDRESS
FROM 
    {{ref('streamline__token_balances_by_date')}}