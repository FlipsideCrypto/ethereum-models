{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash'],
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver_nft__base_arbitrage_trades') }}
),
buy_side AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        bought_usd,
        COALESCE(
            token_swap_amount_in_usd,
            0
        ) AS token_swap_amount_in_usd,
        tx_fee,
        tx_fee_usd
    FROM
        base qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            bought_nft_address,
            bought_tokenid,
            bought_event_index
            ORDER BY
                block_timestamp DESC
        ) = 1
),
buy_side_agg AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        SUM(bought_usd) AS direct_arb_bought_usd,
        SUM(token_swap_amount_in_usd) AS token_swap_usd,
        tx_fee,
        tx_fee_usd
    FROM
        buy_side
    GROUP BY
        ALL
),
sell_side_direct_arb AS (
    SELECT
        tx_hash,
        sold_usd
    FROM
        base qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            sold_nft_address,
            sold_tokenid,
            sold_event_index
            ORDER BY
                block_timestamp DESC
        ) = 1
),
sell_side_direct_arb_agg AS (
    SELECT
        tx_hash,
        SUM(sold_usd) AS direct_arb_sold_usd
    FROM
        sell_side_direct_arb
    GROUP BY
        ALL
),
sell_side_vault_swaps AS (
    SELECT
        tx_hash,
        vault_amount_out_usd
    FROM
        base
    WHERE
        arb_type IN (
            'buy_then_swap',
            'swap_then_buy'
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            vault_swap_index
            ORDER BY
                block_timestamp DESC
        ) = 1
),
sell_side_vault_swaps_agg AS (
    SELECT
        tx_hash,
        SUM(vault_amount_out_usd) AS vault_amount_usd
    FROM
        sell_side_vault_swaps
    GROUP BY
        ALL
),
{# sell_side_token_swaps AS (
SELECT
    tx_hash,
    token_swap_amount_in_usd
FROM
    base
WHERE
    arb_type IN (
        'swap_with_token_then_buy'
    ) qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        token_swap_index
        ORDER BY
            block_timestamp DESC
    ) = 1
),
sell_side_token_swaps_agg AS (
    SELECT
        tx_hash,
        SUM(token_swap_amount_in_usd) AS token_amount_usd
    FROM
        sell_side_token_swaps
    GROUP BY
        ALL
),
#}
sell_side_ape_swaps AS (
    SELECT
        tx_hash,
        ape_amount_usd
    FROM
        base
    WHERE
        ape_swap_index IS NOT NULL qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            ape_swap_index
            ORDER BY
                block_timestamp DESC
        ) = 1
),
sell_side_ape_swaps_agg AS (
    SELECT
        tx_hash,
        SUM(ape_amount_usd) AS ape_amount_usd
    FROM
        sell_side_ape_swaps
    GROUP BY
        ALL
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    direct_arb_bought_usd,
    token_swap_usd,
    direct_arb_bought_usd + token_swap_usd AS total_cost_usd,
    direct_arb_sold_usd,
    vault_amount_usd,
    ape_amount_usd,
    CASE
        WHEN vault_amount_usd IS NOT NULL THEN vault_amount_usd + COALESCE(
            ape_amount_usd,
            0
        )
        WHEN vault_amount_usd IS NULL THEN direct_arb_sold_usd + COALESCE(
            ape_amount_usd,
            0
        )
    END AS total_revenue_usd,
    total_revenue_usd - total_cost_usd AS net_profit_usd,
    tx_fee,
    tx_fee_usd
FROM
    buy_side_agg
    LEFT JOIN sell_side_direct_arb_agg USING (tx_hash)
    LEFT JOIN sell_side_vault_swaps_agg USING (tx_hash)
    LEFT JOIN sell_side_ape_swaps_agg USING (tx_hash)
