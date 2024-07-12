{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_sales AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_name,
        platform_exchange_version,
        nft_address,
        tokenid,
        erc1155_value,
        project_name,
        buyer_address,
        seller_address,
        price,
        total_fees,
        IFF(
            platform_exchange_version = 'nftx'
            AND currency_address = 'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            currency_address
        ) AS currency_address,
        origin_to_address,
        origin_from_address,
        tx_fee,
        input_data,
        modified_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__complete_nft_sales') }}
    WHERE
        (
            CASE
                WHEN price = 0
                AND platform_name = 'larva labs' THEN 0
                ELSE 1
            END
        ) = 1
        AND block_timestamp :: DATE >= '2021-01-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
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
- or bought from , sell to 

for sudoswap - there will be tokens where there are no prices. e.g. swap from weth to snack tokens. 
then use snack tokens to claim from sudoswap pools 

only bought sudoswap needed. sell platform = sudoswap, all got prices. But need to still add just in case 
cryptopunks and nftx 

*/
fees AS (
    SELECT
        tx_hash,
        origin_to_address,
        origin_from_address,
        tx_fee,
        input_data,
        _inserted_timestamp
    FROM
        raw_sales qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index DESC,
                modified_timestamp DESC
        ) = 1
),
base AS (
    SELECT
        b1.block_number,
        b1.block_timestamp,
        b1.tx_hash,
        b1.event_index AS buy_event_index,
        b2.event_index AS sell_event_index,
        b1.nft_address AS buy_nft_address,
        b1.tokenid AS buy_tokenid,
        b1.erc1155_value AS buy_erc1155_value,
        b2.nft_address AS sell_nft_address,
        b2.tokenid AS sell_tokenid,
        b2.erc1155_value AS sell_erc1155_value,
        b1.project_name AS buy_project_name,
        b2.project_name AS sell_project_name,
        b1.buyer_address AS buy_buyer_address,
        b1.seller_address AS buy_seller_address,
        b2.buyer_address AS sell_buyer_address,
        b2.seller_address AS sell_seller_address,
        b1.price AS buy_price,
        b2.price - b2.total_fees AS sell_price,
        CASE
            WHEN b1.platform_exchange_version LIKE 'seaport%'
            AND b1.price = 0
            AND b1.currency_address IS NULL THEN 'ETH'
            ELSE b1.currency_address
        END AS buy_currency,
        CASE
            WHEN b2.platform_exchange_version LIKE 'seaport%'
            AND b2.price = 0
            AND b2.currency_address IS NULL THEN 'ETH'
            ELSE b2.currency_address
        END AS sell_currency,
        b1.platform_name AS buy_platform_name,
        b2.platform_name AS sell_platform_name,
        b1.platform_exchange_version AS buy_platform_exchange_version,
        b2.platform_exchange_version AS sell_platform_exchange_version,
        b1.event_type AS buy_event_type,
        b2.event_type AS sell_event_type,
        f.origin_to_address,
        f.origin_from_address,
        f.tx_fee,
        f.input_data,
        f._inserted_timestamp
    FROM
        raw_sales b1
        INNER JOIN raw_sales b2
        ON b1.tx_hash = b2.tx_hash
        AND b1.buyer_address = b2.seller_address
        AND b1.nft_address = b2.nft_address
        AND b1.tokenid = b2.tokenid
        AND b2.event_index > b1.event_index
        INNER JOIN fees f
        ON f.tx_hash = b1.tx_hash
    WHERE
        b1.buyer_address != b1.seller_address
),
swap_sales AS (
    SELECT
        tx_hash,
        buy_nft_address AS nft_address,
        buy_tokenid AS tokenid,
        buy_buyer_address AS from_address,
        sell_platform_name,
        buy_platform_name
    FROM
        base
    WHERE
        1 = 1 --and tx_hash in (
        --'0x9626f204de0108cf09299d40e5014ee65925b6cb6dc860528454319892da315d'
        -- '0x86b78426891259d00b63f2956fde95e218b46725eddf5e38545afd286cd852e5'
        -- ,'0x1f2c3647517148e2d6898a6ff23d962b335a3e24bb529e86dcda2b40ad736f8a'
        -- ,'0x5439c1210b4f9c6473ed62620308c6574f12d31b9a6d4e8c6848ec587a8757be'
        -- )
        --and sell_platform_name = 'nftx'
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
        block_timestamp :: DATE >= '2021-01-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
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
        to_address AS vault_token
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
        COUNT(1) AS txs
    FROM
        filtered_transfers
    GROUP BY
        ALL
),
erc20_token_swaps AS (
    SELECT
        tx_hash,
        event_index,
        sender,
        tx_to,
        amount_in,
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
        block_timestamp :: DATE >= '2021-01-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
final_vault_swaps AS (
    -- for sales where a vault token is received and sold
    SELECT
        tx_hash,
        event_index AS vault_swap_index,
        amount_out AS vault_amount_out,
        platform AS vault_swap_platform,
        token_out AS vault_token_out,
        symbol_out AS vault_symbol_out
    FROM
        erc20_token_swaps
        INNER JOIN transfers_agg t USING (
            tx_hash,
            token_in
        )
),
ape_token_swaps AS (
    -- for sales where ape tokens are redeemed and sold
    SELECT
        tx_hash,
        event_index AS ape_swap_index,
        amount_out AS ape_amount_out,
        -- amount_out_usd AS ape_amount_out_usd,
        amount_in AS ape_amount_in,
        -- amount_in_usd AS ape_amount_in_usd,
        platform AS ape_sell_platform,
        token_out AS ape_token_out,
        symbol_out AS ape_symbol_out,
        token_in AS ape_token_in,
        symbol_in AS ape_symbol_in,
        --COALESCE(amount_out_usd,ape_amount_in_usd ) AS ape_amount_usd
        IFF(
            amount_out_usd IS NULL,
            ape_amount_in,
            ape_amount_out
        ) AS ape_amount,
        IFF(
            amount_out_usd IS NULL,
            ape_token_in,
            ape_token_out
        ) AS ape_token,
        IFF(
            amount_out_usd IS NULL,
            ape_symbol_in,
            ape_symbol_out
        ) AS ape_symbol
    FROM
        erc20_token_swaps
    WHERE
        token_in = '0x4d224452801aced8b2f0aebe155379bb5d594381'
)
SELECT
    block_number,
    block_timestamp,
    b.tx_hash,
    buy_event_index,
    sell_event_index,
    buy_nft_address,
    buy_tokenid,
    buy_erc1155_value,
    sell_nft_address,
    sell_tokenid,
    sell_erc1155_value,
    buy_project_name,
    sell_project_name,
    buy_buyer_address,
    buy_seller_address,
    sell_buyer_address,
    sell_seller_address,
    buy_price,
    sell_price,
    buy_currency,
    sell_currency,
    buy_platform_name,
    sell_platform_name,
    buy_platform_exchange_version,
    sell_platform_exchange_version,
    buy_event_type,
    sell_event_type,
    vault_swap_index,
    vault_amount_out,
    vault_swap_platform,
    vault_token_out,
    vault_symbol_out,
    t1.event_index AS token_swap_index,
    t1.amount_in AS token_swap_amount_in,
    t1.platform AS token_swap_platform,
    t1.token_in AS token_swap_token_in,
    t1.symbol_in AS token_swap_symbol_in,
    ape_swap_index,
    ape_amount,
    ape_sell_platform,
    ape_token,
    ape_symbol,
    CASE
        WHEN vault_swap_index IS NOT NULL
        AND vault_swap_index > buy_event_index THEN 'buy_then_swap' -- buy nft, redeem token with nft, swap token for weth
        WHEN vault_swap_index IS NOT NULL
        AND vault_swap_index < buy_event_index THEN 'swap_then_buy' -- take a flash loan of token, swap token for weth, use weth to buy nft. Redeem nft for token to repay loan
        WHEN token_swap_index IS NOT NULL THEN 'swap_for_token_then_buy' -- swap weth for token and buys nft with token
        WHEN ape_swap_index IS NOT NULL THEN 'ape_token_sales' -- redeeming and selling ape tokens
        ELSE 'direct_arb' -- selling without any nft token representations
    END AS arb_type,
    tx_fee,
    origin_from_address,
    origin_to_address,
    input_data,
    _inserted_timestamp
FROM
    base b
    LEFT JOIN final_vault_swaps s USING(tx_hash) -- for selling the vault token after sending nft to pool
    LEFT JOIN ape_token_swaps A USING (tx_hash)
    LEFT JOIN erc20_token_swaps t1 -- for getting value of vault token buy to redeem from vault
    ON b.tx_hash = t1.tx_hash
    AND b.buy_price = t1.amount_out
    AND b.buy_currency = t1.token_out
