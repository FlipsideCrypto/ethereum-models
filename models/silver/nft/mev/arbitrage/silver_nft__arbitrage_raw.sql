{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
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
        _log_id,
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
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
fees AS (
    SELECT
        tx_hash,
        origin_to_address,
        origin_from_address,
        tx_fee,
        input_data
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
        b1._log_id,
        b1._inserted_timestamp
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
),
swap_transfers AS (
    SELECT
        tx_hash,
        event_index,
        intra_event_index,
        from_address,
        to_address,
        contract_address AS nft_address,
        token_id AS tokenid,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('nft__ez_nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-01-01'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
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
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
        amount_in AS ape_amount_in,
        platform AS ape_sell_platform,
        token_out AS ape_token_out,
        symbol_out AS ape_symbol_out,
        token_in AS ape_token_in,
        symbol_in AS ape_symbol_in,
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
),
nonpool_flashloans AS (
    SELECT
        tx_hash,
        event_index AS np_flashloan_event_index
    FROM
        {{ ref('silver__complete_lending_flashloans') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    event_index AS np_flashloan_event_index
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    contract_address = LOWER('0x1E0447b19BB6EcFdAe1e4AE1694b0C3659614e4e')
    AND block_timestamp :: DATE >= '2021-01-01'
    AND topics [0] :: STRING = '0xbc83c08f0b269b1726990c8348ffdf1ae1696244a14868d766e542a2f18cd7d4'
    AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
nonpool_flashloans_filter AS (
    SELECT
        *
    FROM
        nonpool_flashloans
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                base
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                np_flashloan_event_index ASC
        ) = 1
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
    e1.event_index AS token_swap_index,
    e1.amount_in AS token_swap_amount_in,
    e1.platform AS token_swap_platform,
    e1.token_in AS token_swap_token_in,
    e1.symbol_in AS token_swap_symbol_in,
    e2.event_index AS token_swap_index_flash,
    ape_swap_index,
    ape_amount,
    ape_sell_platform,
    ape_token,
    ape_symbol,
    np_flashloan_event_index,
    CASE
        WHEN vault_swap_index IS NOT NULL
        AND vault_swap_index > buy_event_index THEN 'buy_then_swap' -- buy nft, redeem token with nft, swap token for weth
        WHEN vault_swap_index IS NOT NULL
        AND vault_swap_index < buy_event_index THEN 'swap_then_buy' -- take a flash loan of token, swap token for weth, use weth to buy nft. Redeem nft for token to repay loan
        WHEN token_swap_index IS NOT NULL THEN 'swap_for_token_then_buy' -- swap weth for token and buys nft with token
        WHEN ape_swap_index IS NOT NULL THEN 'ape_token_sales' -- redeeming and selling ape tokens
        WHEN buy_platform_exchange_version = 'nftx'
        AND token_swap_index_flash > sell_event_index THEN 'flash_swap'
        ELSE 'direct_arb' -- selling without any nft token representations
    END AS arb_type,
    CASE
        WHEN arb_type = 'flash_swap' THEN 'flash_swap'
        WHEN arb_type = 'swap_then_buy'
        OR np_flashloan_event_index IS NOT NULL THEN 'flash_loan'
        ELSE 'existing_funds'
    END AS funding_source,
    IFF(
        buy_platform_exchange_version IN (
            'nftx',
            'sudoswap v1',
            'sudoswap v2'
        ),
        'pool',
        'marketplace'
    ) AS buy_platform_label,
    IFF(
        sell_platform_exchange_version IN (
            'nftx',
            'sudoswap v1',
            'sudoswap v2'
        ),
        'pool',
        'marketplace'
    ) AS sell_platform_label,
    buy_platform_label || '_to_' || sell_platform_label AS arbitrage_direction,
    tx_fee,
    origin_from_address,
    origin_to_address,
    input_data,
    CONCAT(
        buy_event_index,
        '-',
        buy_nft_address,
        '-',
        buy_tokenId,
        '-',
        buy_platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    base b
    LEFT JOIN final_vault_swaps s USING(tx_hash) -- for selling the vault token after sending nft to pool
    LEFT JOIN ape_token_swaps A USING (tx_hash)
    LEFT JOIN erc20_token_swaps e1 -- for getting value of vault token buy to redeem from vault
    ON b.tx_hash = e1.tx_hash
    AND b.buy_price = e1.amount_out
    AND b.buy_currency = e1.token_out
    LEFT JOIN erc20_token_swaps e2 -- to check for pool flash swap
    ON b.tx_hash = e2.tx_hash
    AND b.buy_currency = e2.token_in
    LEFT JOIN nonpool_flashloans_filter n -- to check for flash loans from non pools like dydx
    ON b.tx_hash = n.tx_hash
