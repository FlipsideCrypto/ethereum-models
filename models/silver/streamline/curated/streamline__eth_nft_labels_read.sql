{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['streamline_view']
) }}

WITH nft_sales as (
    SELECT 
        nft_address,
        sum(price) as total_sales_in_eth
    FROM
        {{ ref('core__ez_nft_sales') }}
    WHERE
        currency_address in (
            'ETH', 
            lower('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
        )
    
    GROUP BY nft_address

    HAVING total_sales_in_eth >= 100
),

final AS (
    SELECT
        nft_address as contract_address,
        'ethereum_nft_labels' AS call_name
        
    FROM
        nft_min_block
)

SELECT
    {{ dbt_utils.surrogate_key(
        ['contract_address']
    ) }} AS id,
    contract_address,
    call_name
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    call_name DESC)) = 1
