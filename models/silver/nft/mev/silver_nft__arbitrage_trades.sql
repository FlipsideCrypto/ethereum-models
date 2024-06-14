{{ config(
    materialized = 'table',
    unique_key = ['block_number', 'bought_event_index', 'nft_address', 'tokenid'],
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_sales AS (

    SELECT
        *
    FROM
        {{ ref('nft__ez_nft_sales') }}
)
/*
assumptions and definitions 
only same tx
same block 

erc1155 just count as per tx profit - not going to look at per nft profit because there can be cases 
- per nft profit and TOTAL profit 

maybe add a total profit for APE claiming 

two sides 
- maybe call it buy from , sell to 
- or bought from , sold to 

for sudoswap - there will be tokens where there are no prices. e.g. swap from weth to snack tokens. 
then use snack tokens to claim from sudoswap pools 

only bought sudoswap needed. Sold platform = sudoswap, all got prices. But need to still add just in case 
cryptopunks and nftx 

*/
SELECT
    b1.block_number,
    b1.block_timestamp,
    b1.tx_hash,
    b1.event_index AS bought_event_index,
    b2.event_index AS sold_event_index,
    b1.nft_address,
    b1.tokenid,
    b1.erc1155_value,
    b2.erc1155_value AS sold_erc1155_value,
    b1.project_name,
    b1.buyer_address,
    b1.price AS bought,
    b2.price AS sold,
    b1.currency_address AS bought_currency,
    b2.currency_address AS sold_currency,
    b1.platform_name AS bought_platform_name,
    b2.platform_name AS sold_platform_name,
    b1.event_type AS bought_event_type,
    b2.event_type AS sold_event_type,
    b1.price_usd AS bought_usd,
    b2.price_usd AS sold_usd,
    b1.tx_fee,
    b1.tx_fee_usd,
    sold_usd - (
        bought_usd + b1.tx_fee_usd
    ) AS profit_usd
FROM
    base_sales b1
    INNER JOIN base_sales b2
    ON b1.tx_hash = b2.tx_hash
    AND b1.buyer_address = b2.seller_address
    AND b1.nft_address = b2.nft_address
    AND b1.tokenid = b2.tokenid
    AND b2.event_index > b1.event_index
WHERE
    1 = 1 --    b1.erc1155_value IS NULL
    AND b1.buyer_address != b1.seller_address
    AND b1.platform_exchange_version NOT IN (
        'nftx',
        'cryptopunks'
    )
