{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash', 'bought_event_index', 'bought_nft_address', 'bought_tokenid'],
    cluster_by = ['block_timestamp::DATE']
) }}

WITH raw_sales AS (

    SELECT
        *
    FROM
        {{ ref('nft__ez_nft_sales') }}
    WHERE
        (
            CASE
                WHEN price = 0
                AND platform_name = 'larva labs' THEN 0
                ELSE 1
            END
        ) = 1
),
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
base AS (
    SELECT
        b1.block_number,
        b1.block_timestamp,
        b1.tx_hash,
        b1.event_index AS bought_event_index,
        b2.event_index AS sold_event_index,
        b1.nft_address AS bought_nft_address,
        b1.tokenid AS bought_tokenid,
        b1.erc1155_value AS bought_erc1155_value,
        b2.nft_address AS sold_nft_address,
        b2.tokenid AS sold_tokenid,
        b2.erc1155_value AS sold_erc1155_value,
        b1.project_name AS bought_project_name,
        b2.project_name AS sold_project_name,
        b1.buyer_address,
        b1.price AS bought,
        b2.price AS sold,
        CASE
            WHEN b1.platform_exchange_version LIKE 'seaport%'
            AND b1.price = 0
            AND b1.currency_address IS NULL THEN 'ETH'
            ELSE b1.currency_address
        END AS bought_currency,
        b2.currency_address AS sold_currency,
        b1.platform_name AS bought_platform_name,
        b2.platform_name AS sold_platform_name,
        b1.event_type AS bought_event_type,
        b2.event_type AS sold_event_type,
        b1.price_usd AS bought_usd,
        b2.price_usd AS sold_usd,
        b1.tx_fee,
        b1.tx_fee_usd
    FROM
        raw_sales b1
        INNER JOIN raw_sales b2
        ON b1.tx_hash = b2.tx_hash
        AND b1.buyer_address = b2.seller_address
        AND b1.nft_address = b2.nft_address
        AND b1.tokenid = b2.tokenid
        AND b2.event_index > b1.event_index
    WHERE
        1 = 1 --    b1.erc1155_value IS NULL
        AND b1.buyer_address != b1.seller_address {# AND b1.platform_exchange_version NOT IN (
        'nftx',
        'cryptopunks'
) #}
),
swap_sales AS (
    SELECT
        tx_hash,
        bought_nft_address AS nft_address,
        bought_tokenid AS tokenid,
        buyer_address AS from_address,
        sold_platform_name,
        bought_platform_name
    FROM
        base
    WHERE
        1 = 1 --and tx_hash in (
        --'0x9626f204de0108cf09299d40e5014ee65925b6cb6dc860528454319892da315d'
        -- '0x86b78426891259d00b63f2956fde95e218b46725eddf5e38545afd286cd852e5'
        -- ,'0x1f2c3647517148e2d6898a6ff23d962b335a3e24bb529e86dcda2b40ad736f8a'
        -- ,'0x5439c1210b4f9c6473ed62620308c6574f12d31b9a6d4e8c6848ec587a8757be'
        -- )
        --and sold_platform_name = 'nftx'
),
swap_transfers AS (
    SELECT
        tx_hash,
        event_index,
        intra_event_index,
        from_address,
        to_address,
        contract_address AS nft_address,
        tokenid,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-03-01'
),
filtered_transfers AS (
    SELECT
        tx_hash,
        event_index,
        intra_event_index,
        nft_address,
        tokenid,
        from_address,
        to_address,
        to_address AS vault_token,
        sold_platform_name,
        bought_platform_name
    FROM
        swap_sales
        INNER JOIN swap_transfers USING (
            tx_hash,
            nft_address,
            tokenid,
            from_address
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index,
            intra_event_index
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
transfers_agg AS (
    SELECT
        tx_hash,
        vault_token AS token_in,
        sold_platform_name,
        bought_platform_name,
        COUNT(1)
    FROM
        filtered_transfers
    GROUP BY
        ALL
),
token_swaps AS (
    SELECT
        tx_hash,
        event_index,
        sender,
        tx_to,
        amount_in,
        amount_in_usd,
        amount_out,
        amount_out_usd,
        platform,
        token_in,
        token_out,
        symbol_in,
        symbol_out
    FROM
        {{ ref('silver_dex__complete_dex_swaps') }}
    WHERE
        block_timestamp :: DATE >= '2021-03-01'
),
final_vault_swaps AS (
    -- for sales where a vault token is received and sold
    SELECT
        tx_hash,
        event_index AS vault_swap_index,
        amount_out AS vault_amount_out,
        amount_out_usd AS vault_amount_out_usd,
        platform AS vault_swap_platform,
        token_out AS vault_token_out,
        symbol_out AS vault_symbol_out
    FROM
        token_swaps
        INNER JOIN transfers_agg t USING (
            tx_hash,
            token_in
        )
),
ape_token_swaps AS (
    SELECT
        tx_hash,
        event_index AS ape_swap_index,
        amount_out AS ape_amount_out,
        amount_out_usd AS ape_amount_out_usd,
        amount_in AS ape_amount_in,
        amount_in_usd AS ape_amount_in_usd,
        platform AS ape_sold_platform,
        token_out AS ape_token_out,
        symbol_out AS ape_symbol_out,
        token_in AS ape_token_in,
        symbol_in AS ape_symbol_in,
        COALESCE(
            ape_amount_out_usd,
            ape_amount_in_usd
        ) AS ape_amount_usd,
        IFF(
            ape_amount_out_usd IS NULL,
            ape_amount_in,
            ape_amount_out
        ) AS ape_amount,
        IFF(
            ape_amount_out_usd IS NULL,
            ape_token_in,
            ape_token_out
        ) AS ape_token,
        IFF(
            ape_amount_out_usd IS NULL,
            ape_symbol_in,
            ape_symbol_out
        ) AS ape_symbol
    FROM
        token_swaps
    WHERE
        token_in = '0x4d224452801aced8b2f0aebe155379bb5d594381'
)
SELECT
    block_number,
    block_timestamp,
    b.tx_hash,
    bought_event_index,
    sold_event_index,
    bought_nft_address,
    bought_tokenid,
    bought_erc1155_value,
    sold_nft_address,
    sold_tokenid,
    sold_erc1155_value,
    bought_project_name,
    sold_project_name,
    buyer_address,
    bought,
    sold,
    bought_currency,
    sold_currency,
    bought_platform_name,
    sold_platform_name,
    bought_event_type,
    sold_event_type,
    bought_usd,
    sold_usd,
    tx_fee,
    tx_fee_usd,
    vault_swap_index,
    vault_amount_out,
    vault_amount_out_usd,
    vault_swap_platform,
    vault_token_out,
    vault_symbol_out,
    t1.event_index AS token_swap_index,
    t1.amount_in AS token_swap_amount_in,
    t1.amount_in_usd AS token_swap_amount_in_usd,
    t1.platform AS token_swap_platform,
    t1.token_in AS token_swap_token_in,
    t1.symbol_in AS token_swap_symbol_in,
    ape_swap_index,
    ape_amount,
    ape_amount_usd,
    ape_sold_platform,
    ape_token,
    ape_symbol,
    CASE
        WHEN vault_swap_index IS NOT NULL
        AND vault_swap_index > bought_event_index THEN 'buy_then_swap'
        WHEN vault_swap_index IS NOT NULL
        AND vault_swap_index < bought_event_index THEN 'swap_then_buy'
        WHEN token_swap_index IS NOT NULL THEN 'swap_with_token_then_buy'
        WHEN ape_swap_index IS NOT NULL THEN 'ape'
        ELSE 'direct_arb'
    END AS arb_type
FROM
    base b
    LEFT JOIN final_vault_swaps s USING(tx_hash) -- for selling the vault token after sending nft to pool
    LEFT JOIN ape_token_swaps A USING (tx_hash)
    LEFT JOIN token_swaps t1 -- for getting value of vault token bought to redeem from vault
    ON b.tx_hash = t1.tx_hash
    AND b.bought = t1.amount_out
    AND b.bought_currency = t1.token_out
