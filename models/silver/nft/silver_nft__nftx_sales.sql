{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH vaults AS (

    SELECT
        event_inputs :assetAddress :: STRING AS nft_address,
        event_inputs :vaultAddress :: STRING AS nftx_token_address
    FROM
        {{ ref('silver__logs') }}
    WHERE
        event_name = 'NewVault'
        AND contract_address = '0xbe86f647b167567525ccaafcd6f881f1ee558216'
),
nftx_token_swaps AS (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN symbol_in = 'WETH' THEN token_out
            ELSE token_in
        END AS nftx_token,
        CASE
            WHEN symbol_in = 'WETH'
            AND amount_out_usd IS NOT NULL THEN amount_out
            WHEN symbol_in = 'WETH'
            AND amount_out_usd IS NULL
            AND amount_out < 500 THEN amount_out
            WHEN symbol_in = 'WETH'
            AND amount_out_usd IS NULL
            AND amount_out > 500 THEN amount_out / pow(
                10,
                18
            )
            WHEN symbol_in <> 'WETH'
            AND amount_in_usd IS NOT NULL THEN amount_in
            WHEN symbol_in <> 'WETH'
            AND amount_in_usd IS NULL
            AND amount_in < 500 THEN amount_in
            ELSE amount_in / pow(
                10,
                18
            )
        END AS nftx_tokens,
        ROUND(
            CASE
                WHEN symbol_in = 'WETH' THEN amount_in_usd / nftx_tokens
                ELSE amount_out_usd / nftx_tokens
            END,
            2
        ) AS usd_price_per_token
    FROM
        {{ ref('silver_dex__v2_swaps') }}
    WHERE
        nftx_tokens >.05
        AND (
            token_out IN (
                SELECT
                    nftx_token_address
                FROM
                    vaults
            )
            OR token_in IN (
                SELECT
                    nftx_token_address
                FROM
                    vaults
            )
        )
        AND (
            symbol_in = 'WETH'
            OR symbol_out = 'WETH'
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
date_spine AS (
    SELECT
        DISTINCT HOUR,
        nftx_token_address
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
        LEFT JOIN vaults
),
join_dates AS (
    SELECT
        HOUR AS nftx_date,
        date_spine.nftx_token_address AS token,
        usd_price_per_token
    FROM
        date_spine
        LEFT JOIN nftx_token_swaps A
        ON HOUR = DATE_TRUNC(
            'hour',
            block_timestamp
        )
        AND date_spine.nftx_token_address = A.nftx_token
),
fill_dates AS (
    SELECT
        nftx_date,
        token AS nftx_token_address,
        CASE
            WHEN usd_price_per_token IS NULL THEN LAG(usd_price_per_token) ignore nulls over (
                PARTITION BY token
                ORDER BY
                    nftx_date
            )
            ELSE usd_price_per_token
        END AS nftx_token_price
    FROM
        join_dates
),
full_fill_dates AS (
    SELECT
        nftx_date,
        nftx_token_address,
        COALESCE(
            CASE
                WHEN nftx_token_price IS NULL THEN FIRST_VALUE(
                    nftx_token_price ignore nulls
                ) over (
                    PARTITION BY nftx_token_address
                    ORDER BY
                        nftx_date
                )
                ELSE nftx_token_price
            END,
            0
        ) AS nftx_token_price
    FROM
        fill_dates
),
base_sale_logs AS (
    SELECT
        _log_id,
        block_timestamp,
        tx_hash,
        event_inputs,
        contract_address AS platform_address,
        nft_address,
        nftx_token_address
    FROM
        {{ ref('silver__logs') }}
        LEFT JOIN vaults
        ON contract_address = nftx_token_address
    WHERE
        event_name = 'Redeemed'
        AND contract_address IN (
            SELECT
                nftx_token_address
            FROM
                vaults
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
flat_sales_logs AS (
    SELECT
        _log_id,
        block_timestamp,
        tx_hash,
        nft_address,
        nftx_token_address,
        platform_address,
        f.value :: STRING AS tokenId
    FROM
        base_sale_logs,
        TABLE(FLATTEN(base_sale_logs.event_inputs :nftIds)) f
),
last_nft_transfer AS (
    SELECT
        A._log_id,
        A.tx_hash,
        A.block_timestamp,
        A.contract_address,
        A.project_name,
        A.from_address,
        A.to_address,
        A.tokenid,
        A.erc1155_value,
        A.token_metadata,
        A.ingested_at,
        A.event_index,
        b.nftx_token_address,
        b.platform_address
    FROM
        {{ ref('silver__nft_transfers') }} A
        INNER JOIN flat_sales_logs b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.nft_address
        AND A.tokenId = b.tokenId

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY A.tx_hash, A.contract_address, A.tokenid
ORDER BY
    event_index DESC)) = 1
),
first_nft_transfer AS (
    SELECT
        A._log_id,
        A.tx_hash,
        A.block_timestamp,
        A.contract_address,
        A.project_name,
        A.from_address,
        A.to_address,
        A.tokenid,
        A.erc1155_value,
        A.token_metadata,
        A.ingested_at,
        A.event_index,
        b.nftx_token_address,
        b.platform_address
    FROM
        {{ ref('silver__nft_transfers') }} A
        INNER JOIN flat_sales_logs b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.nft_address
        AND A.tokenId = b.tokenId

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY A.tx_hash, A.contract_address, A.tokenid
ORDER BY
    event_index ASC)) = 1
),
nft_transfers AS (
    SELECT
        A._log_id,
        A.tx_hash,
        A.block_timestamp,
        A.contract_address,
        A.project_name,
        b.from_address,
        A.to_address,
        A.tokenid,
        A.erc1155_value,
        A.token_metadata,
        A.ingested_at,
        A.event_index,
        b.nftx_token_address,
        b.platform_address
    FROM
        last_nft_transfer A
        JOIN first_nft_transfer b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.contract_address
        AND A.tokenid = b.tokenid
),
token_transfers AS (
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        raw_amount / pow(
            10,
            18
        ) AS adj_amount,
        CASE
            WHEN to_address = '0x0000000000000000000000000000000000000000' THEN 'sale_amt'
            WHEN to_address IN (
                '0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86',
                '0x40d73df4f99bae688ce3c23a01022224fe16c7b2',
                '0x7ae9d7ee8489cad7afc84111b8b185ee594ae090'
            ) THEN 'fees'
        END AS token_type,
        b.nft_address
    FROM
        {{ ref('silver__transfers') }}
        LEFT JOIN vaults b
        ON contract_address = b.nftx_token_address
    WHERE
        adj_amount < 500
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                nft_transfers
        )
        AND contract_address IN (
            SELECT
                DISTINCT nftx_token_address
            FROM
                nft_transfers
        )
        AND token_type IS NOT NULL

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
sale_count AS (
    SELECT
        tx_hash,
        contract_address,
        COUNT(*) AS nft_transfers
    FROM
        nft_transfers
    GROUP BY
        tx_hash,
        contract_address
),
total_sales AS (
    SELECT
        tx_hash,
        contract_address,
        SUM(adj_amount) AS sale_amount
    FROM
        token_transfers
    WHERE
        token_type = 'sale_amt'
    GROUP BY
        tx_hash,
        contract_address
),
total_fees AS (
    SELECT
        tx_hash,
        contract_address,
        AVG(adj_amount) AS fee_amount
    FROM
        token_transfers
    WHERE
        token_type = 'fees'
    GROUP BY
        tx_hash,
        contract_address
),
final_base AS (
    SELECT
        A.*,
        b.nft_transfers,
        (COALESCE(C.sale_amount, 0) + COALESCE(d.fee_amount, 0)) / b.nft_transfers AS price,
        COALESCE(
            d.fee_amount,
            0
        ) / b.nft_transfers AS fees
    FROM
        nft_transfers A
        LEFT JOIN sale_count b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.contract_address
        LEFT JOIN total_sales C
        ON A.tx_hash = C.tx_hash
        AND A.nftx_token_address = C.contract_address
        LEFT JOIN total_fees d
        ON A.tx_hash = d.tx_hash
        AND A.nftx_token_address = d.contract_address
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address,
        from_address,
        eth_value,
        tx_fee,
        origin_function_signature,
        ingested_at
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                final_base
        )

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
eth_token_price AS (
    SELECT
        HOUR,
        AVG(price) AS eth_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IS NULL
        AND symbol IS NULL
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
    GROUP BY
        HOUR,
        token_address
),
final_table AS (
    SELECT
        A._log_id AS _log_id,
        A.tx_hash AS tx_hash,
        A.block_timestamp AS block_timestamp,
        A.contract_address AS nft_address,
        A.project_name AS project_name,
        A.from_address AS seller_address,
        A.to_address AS buyer_address,
        A.tokenid AS tokenId,
        A.erc1155_value AS erc1155_value,
        A.token_metadata AS token_metadata,
        A.ingested_at AS ingested_at,
        A.price AS price,
        COALESCE(
            A.fees,
            0
        ) AS total_fees,
        COALESCE(
            A.fees,
            0
        ) AS platform_fee,
        0 AS creator_fee,
        0 AS creator_fee_usd,
        tx.to_address AS origin_to_address,
        tx.from_address AS origin_from_address,
        tx.origin_function_signature AS origin_function_signature,
        tx.tx_fee AS tx_fee,
        ROUND(
            tx.tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        A.nftx_token_address AS currency_address,
        'nftx_token' AS currency_symbol,
        'redeem' AS event_type,
        tx.block_number,
        'nftx' AS platform_name,
        A.platform_address AS platform_address,
        ROUND(
            nftx_token_price * A.price,
            2
        ) AS price_usd,
        ROUND(nftx_token_price * COALESCE(A.fees, 0), 2) AS total_fees_usd,
        ROUND(nftx_token_price * COALESCE(A.fees, 0), 2) AS platform_fee_usd
    FROM
        final_base A
        LEFT JOIN tx_data tx
        ON A.tx_hash = tx.tx_hash
        LEFT JOIN eth_token_price
        ON DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = eth_token_price.hour
        LEFT JOIN full_fill_dates f
        ON DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = f.nftx_date
        AND A.nftx_token_address = f.nftx_token_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    event_type,
    platform_address,
    platform_name,
    buyer_address,
    seller_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    _log_id,
    ingested_at
FROM
    final_table qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    ingested_at DESC)) = 1
