{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver_nft__arbitrage_raw') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
prices_raw AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        HOUR :: DATE >= '2021-01-01'
        AND HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base
        )
        AND token_address IN (
            SELECT
                buy_currency
            FROM
                base
            UNION ALL
            SELECT
                sell_currency
            FROM
                base
            UNION ALL
            SELECT
                vault_token_out
            FROM
                base
            UNION ALL
            SELECT
                token_swap_token_in
            FROM
                base
            UNION ALL
            SELECT
                ape_token
            FROM
                base
        )
),
all_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        '0x0000000000a39bb272e79075ade125fd351887ac' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
eth_price AS (
    SELECT
        HOUR,
        hourly_prices AS eth_price_hourly
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
base_with_prices AS (
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
        buyer_address,
        buy_price,
        sell_price,
        buy_currency,
        sell_currency,
        buy_price * p1.hourly_prices AS buy_price_usd,
        sell_price * p2.hourly_prices AS sell_price_usd,
        buy_platform_name,
        sell_platform_name,
        buy_event_type,
        sell_event_type,
        vault_swap_index,
        vault_amount_out,
        vault_amount_out * p3.hourly_prices AS vault_amount_out_usd,
        vault_swap_platform,
        vault_token_out,
        vault_symbol_out,
        token_swap_index,
        token_swap_amount_in,
        token_swap_amount_in * p4.hourly_prices AS token_swap_amount_in_usd,
        token_swap_platform,
        token_swap_token_in,
        token_swap_symbol_in,
        ape_swap_index,
        ape_amount,
        ape_amount * p5.hourly_prices AS ape_amount_usd,
        ape_sell_platform,
        ape_token,
        ape_symbol,
        arb_type,
        tx_fee,
        tx_fee * eth_price_hourly AS tx_fee_usd,
        origin_from_address,
        origin_to_address,
        input_data,
        _inserted_timestamp
    FROM
        base b
        LEFT JOIN all_prices p1
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p1.hour
        AND b.buy_currency = p1.token_address
        LEFT JOIN all_prices p2
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p2.hour
        AND b.sell_currency = p2.token_address
        LEFT JOIN all_prices p3
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p3.hour
        AND b.vault_token_out = p3.token_address
        LEFT JOIN all_prices p4
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p4.hour
        AND b.token_swap_token_in = p4.token_address
        LEFT JOIN all_prices p5
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p5.hour
        AND b.ape_token = p5.token_address
        LEFT JOIN eth_price e
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = e.hour
),
buy_side AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        buy_currency,
        buy_price,
        buy_price_usd,
        token_swap_token_in,
        token_swap_amount_in,
        token_swap_amount_in_usd,
        tx_fee,
        tx_fee_usd,
        origin_from_address,
        origin_to_address,
        input_data,
        _inserted_timestamp
    FROM
        base_with_prices qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            buy_nft_address,
            buy_tokenid,
            buy_event_index
            ORDER BY
                block_timestamp DESC
        ) = 1
),
buy_side_agg AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_fee,
        tx_fee_usd,
        origin_from_address,
        origin_to_address,
        input_data,
        _inserted_timestamp,
        SUM(buy_price_usd) AS direct_arb_buy_usd,
        SUM(token_swap_amount_in_usd) AS token_swap_usd
    FROM
        buy_side
    GROUP BY
        ALL
),
sell_side_direct_arb AS (
    SELECT
        tx_hash,
        sell_currency,
        sell_price,
        IFF(
            sell_platform_name = 'nftx'
            AND vault_amount_out_usd IS NOT NULL,
            0,
            sell_price_usd
        ) AS sell_price_usd
    FROM
        base_with_prices qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            sell_nft_address,
            sell_tokenid,
            sell_event_index
            ORDER BY
                block_timestamp DESC
        ) = 1
),
sell_side_direct_arb_agg AS (
    SELECT
        tx_hash,
        SUM(sell_price_usd) AS direct_arb_sell_usd
    FROM
        sell_side_direct_arb
    GROUP BY
        ALL
),
sell_side_vault_swaps AS (
    SELECT
        tx_hash,
        vault_token_out,
        vault_amount_out,
        vault_amount_out_usd
    FROM
        base_with_prices
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
sell_side_ape_swaps AS (
    SELECT
        tx_hash,
        ape_token,
        ape_amount,
        ape_amount_usd
    FROM
        base_with_prices
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
    direct_arb_buy_usd,
    token_swap_usd,
    IFF(
        direct_arb_buy_usd IS NULL,
        token_swap_usd,
        direct_arb_buy_usd
    ) AS total_cost_usd,
    direct_arb_sell_usd,
    vault_amount_usd,
    ape_amount_usd,
    COALESCE(
        vault_amount_usd,
        0
    ) + COALESCE(
        ape_amount_usd,
        0
    ) + COALESCE(
        direct_arb_sell_usd,
        0
    ) AS total_revenue_usd,
    total_revenue_usd - total_cost_usd AS net_profit_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_from_address AS mev_searcher,
    origin_to_address AS mev_contract,
    input_data,
    _inserted_timestamp -- add PK
FROM
    buy_side_agg
    LEFT JOIN sell_side_direct_arb_agg USING (tx_hash)
    LEFT JOIN sell_side_vault_swaps_agg USING (tx_hash)
    LEFT JOIN sell_side_ape_swaps_agg USING (tx_hash)
