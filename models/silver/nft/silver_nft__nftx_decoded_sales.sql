{{ config(
    materialized = 'incremental',
    unique_key = 'log_id_nft',
    cluster_by = ['block_timestamp::DATE']
) }}
/*
this model includes :
1. direct redeems from vault from nftx site 
2. swap ETH to NFT (buy and redeem) via marketplace zap & custom contracts 
3. sell NFT to eth (mint and sell) via marketplace zap & custom contracts
*/
WITH vaults AS (

    SELECT
        decoded_flat :assetAddress :: STRING AS nft_address,
        decoded_flat :vaultAddress :: STRING AS vault_address,
        decoded_flat :vaultId :: STRING AS vault_id
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 12676663
        AND contract_address = '0xbe86f647b167567525ccaafcd6f881f1ee558216'
        AND event_name = 'NewVault'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
direct_vault_redeems AS (
    SELECT
        tx_hash
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        origin_function_signature = '0xc4a0db96'
        AND block_number >= 12676663
        AND to_address IN (
            SELECT
                vault_address
            FROM
                vaults
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
redeem_txs AS (
    SELECT
        tx_hash,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        decoded_flat,
        ARRAY_SIZE(
            decoded_flat :nftIds
        ) AS total_nfts,
        ARRAY_SIZE(
            decoded_flat :specificIds
        ) AS total_specific_nfts,
        l.contract_address AS vault_address,
        nft_address,
        decoded_flat :to :: STRING AS nft_receiver
    FROM
        {{ ref('silver__decoded_logs') }}
        l
        INNER JOIN vaults v
        ON l.contract_address = v.vault_address
    WHERE
        block_number >= 12676663
        AND event_name IN (
            'Redeemed'
        )
        AND contract_address IN (
            SELECT
                vault_address
            FROM
                vaults
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
all_swaps AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        sender,
        tx_to,
        contract_address AS pool_address,
        token_in AS currency_address,
        amount_in AS price_adj,
        token_out AS received_token_address,
        amount_out AS received_price_adj
    FROM
        {{ ref('silver_dex__complete_dex_swaps') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND (
            token_in IN (
                SELECT
                    vault_address
                FROM
                    vaults
            )
            OR token_out IN (
                SELECT
                    vault_address
                FROM
                    vaults
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
redeem_txs_all_nft_id AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        _log_id,
        _inserted_timestamp,
        vault_address,
        nft_address,
        VALUE AS tokenid,
        nft_receiver
    FROM
        redeem_txs,
        LATERAL FLATTEN(
            input => decoded_flat :nftIds
        )
),
redeem_txs_direct_vault_nft_price AS (
    SELECT
        l.block_number,
        l.tx_hash,
        l.contract_address AS vault_address,
        l.decoded_flat :from :: STRING AS from_address,
        l.decoded_flat :to :: STRING AS to_address,
        l.decoded_flat :value / pow(
            10,
            18
        ) AS amount,
        IFF(
            to_address = '0x0000000000000000000000000000000000000000',
            amount / total_nfts,
            0
        ) AS nft_price,
        IFF(
            to_address IN (
                '0x7ae9d7ee8489cad7afc84111b8b185ee594ae090',
                '0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86'
            ),
            amount / total_nfts,
            0
        ) AS vault_fee,
        IFF(
            from_address = '0x0000000000000000000000000000000000000000',
            amount / total_nfts,
            0
        ) AS extra_platform_fee_token_minted_raw,
        IFF(
            to_address = '0x40d73df4f99bae688ce3c23a01022224fe16c7b2',
            amount / total_nfts,
            0
        ) AS platform_fee_raw
    FROM
        {{ ref('silver__decoded_logs') }}
        l
        INNER JOIN redeem_txs r
        ON l.tx_hash = r.tx_hash
        AND l.contract_address = r.vault_address
    WHERE
        block_number >= 12676663
        AND l.event_name = 'Transfer'
        AND l.tx_hash IN (
            SELECT
                tx_hash
            FROM
                direct_vault_redeems
        )
        AND amount IS NOT NULL
        AND to_address IN (
            '0x0000000000000000000000000000000000000000',
            '0x7ae9d7ee8489cad7afc84111b8b185ee594ae090',
            '0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86',
            '0x40d73df4f99bae688ce3c23a01022224fe16c7b2'
        )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
redeem_txs_direct_vault_nft_price_agg AS (
    SELECT
        block_number,
        tx_hash,
        vault_address,
        SUM(nft_price) AS price_raw,
        SUM(vault_fee) AS creator_fee_raw,
        SUM(extra_platform_fee_token_minted_raw) AS extra_platform_fee_token_minted,
        SUM(platform_fee_raw) AS platform_fee
    FROM
        redeem_txs_direct_vault_nft_price
    GROUP BY
        block_number,
        tx_hash,
        vault_address
),
redeem_txs_direct_vault_base AS (
    -- final base for direct redeem
    SELECT
        p.block_number,
        l.tx_hash,
        l.event_index,
        l.event_name,
        'redeem' AS event_type,
        l.vault_address AS contract_address,
        l.vault_address AS currency_address,
        -- the vault address is the same address as the token (e.g. PHUNK token)
        l.vault_address AS seller_address,
        l.nft_receiver AS buyer_address,
        l.nft_address,
        l.tokenid,
        p.price_raw - p.extra_platform_fee_token_minted AS price,
        p.creator_fee_raw - p.platform_fee AS creator_fee,
        p.platform_fee,
        creator_fee + p.platform_fee AS total_fees,
        l._log_id,
        l._inserted_timestamp
    FROM
        redeem_txs_all_nft_id l
        INNER JOIN redeem_txs_direct_vault_nft_price_agg p
        ON l.tx_hash = p.tx_hash
        AND l.vault_address = p.vault_address
),
swap_eth_for_nft_from_vault AS (
    SELECT
        tx_hash,
        currency_address,
        received_token_address,
        SUM(price_adj) AS total_price_adj
    FROM
        all_swaps
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                redeem_txs
        )
        AND currency_address IN (
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        )
    GROUP BY
        tx_hash,
        currency_address,
        received_token_address
),
swap_eth_for_nft_from_vault_nft_price AS (
    SELECT
        l.block_number,
        l.tx_hash,
        l.contract_address AS vault_address,
        l.decoded_flat,
        l.decoded_flat :from :: STRING AS from_address,
        l.decoded_flat :to :: STRING AS to_address,
        l.decoded_flat :value / pow(
            10,
            18
        ) AS amount,
        total_nfts,
        IFF(
            to_address = '0x0000000000000000000000000000000000000000',
            amount / total_nfts,
            0
        ) AS nft_price,
        IFF(
            to_address IN (
                '0x7ae9d7ee8489cad7afc84111b8b185ee594ae090',
                '0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86'
            ),
            amount / total_nfts,
            0
        ) AS total_fees_raw,
        IFF(
            to_address = '0x40d73df4f99bae688ce3c23a01022224fe16c7b2',
            amount / total_nfts,
            0
        ) AS platform_fee_raw
    FROM
        {{ ref('silver__decoded_logs') }}
        l
        INNER JOIN redeem_txs r
        ON l.tx_hash = r.tx_hash
        AND l.contract_address = r.vault_address
    WHERE
        block_number >= 12676663
        AND l.event_name = 'Transfer'
        AND l.tx_hash IN (
            SELECT
                tx_hash
            FROM
                swap_eth_for_nft_from_vault
        )
        AND amount IS NOT NULL
        AND to_address IN (
            '0x0000000000000000000000000000000000000000',
            '0x7ae9d7ee8489cad7afc84111b8b185ee594ae090',
            '0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86',
            '0x40d73df4f99bae688ce3c23a01022224fe16c7b2'
        )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
swap_eth_for_nft_from_vault_nft_price_agg AS (
    SELECT
        block_number,
        tx_hash,
        vault_address,
        total_nfts,
        SUM(nft_price) AS nft_price_adj,
        SUM(total_fees_raw) AS total_fees_adj,
        SUM(platform_fee_raw) AS platform_fee_adj
    FROM
        swap_eth_for_nft_from_vault_nft_price
    GROUP BY
        block_number,
        tx_hash,
        vault_address,
        total_nfts
),
swap_eth_for_nft_from_vault_base AS (
    -- final base for swap eth for eth from vault
    SELECT
        p.block_number,
        l.tx_hash,
        l.vault_address,
        l.vault_address AS seller_address,
        l.nft_receiver AS buyer_address,
        l.nft_address,
        l.tokenid,
        p.total_nfts,
        p.nft_price_adj,
        p.total_fees_adj,
        p.platform_fee_adj,
        p.total_fees_adj - p.platform_fee_adj AS creator_fee_adj,
        p.nft_price_adj + p.total_fees_adj AS price_per_nft,
        e.currency_address,
        e.total_price_adj,
        e.total_price_adj / (
            p.total_nfts
        ) AS price_in_eth_per_nft,
        price_in_eth_per_nft / price_per_nft * creator_fee_adj AS creator_fee_in_eth,
        price_in_eth_per_nft / price_per_nft * p.platform_fee_adj AS platform_fee_in_eth,
        platform_fee_in_eth + creator_fee_in_eth AS total_fees_in_eth,
        r.event_index,
        r._log_id,
        r._inserted_timestamp,
        r.event_name,
        'sale' AS event_type
    FROM
        redeem_txs_all_nft_id l
        INNER JOIN swap_eth_for_nft_from_vault_nft_price_agg p
        ON l.tx_hash = p.tx_hash
        AND l.vault_address = p.vault_address
        INNER JOIN swap_eth_for_nft_from_vault e
        ON l.tx_hash = e.tx_hash
        AND l.vault_address = e.received_token_address
        INNER JOIN redeem_txs r
        ON l.tx_hash = r.tx_hash
        AND l.vault_address = r.vault_address
),
swap_nft_for_eth_from_vault_nft_price AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address,
        COALESCE (
            decoded_flat :from,
            decoded_flat :src
        ) :: STRING AS from_address,
        COALESCE (
            decoded_flat :to,
            decoded_flat :dst
        ) :: STRING AS to_address,
        COALESCE (
            decoded_flat :value,
            decoded_flat :wad
        ) :: INT AS VALUE,
        IFF(
            from_address = '0x0000000000000000000000000000000000000000'
            AND contract_address IN (
                SELECT
                    vault_address
                FROM
                    vaults
            ),
            VALUE / pow(
                10,
                18
            ),
            0
        ) AS nft_tokens_created,
        IFF(
            to_address IN (
                '0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86',
                '0x7ae9d7ee8489cad7afc84111b8b185ee594ae090'
            ),
            VALUE / pow(
                10,
                18
            ),
            0
        ) AS total_fees,
        IFF(
            to_address = '0x40d73df4f99bae688ce3c23a01022224fe16c7b2'
            AND contract_address IN (
                SELECT
                    vault_address
                FROM
                    vaults
            ),
            VALUE / pow(
                10,
                18
            ),
            0
        ) AS platform_fees,
        IFF(
            contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            AND from_address IN (
                SELECT
                    pool_address
                FROM
                    all_swaps
            ),
            VALUE / pow(
                10,
                18
            ),
            0
        ) AS weth_received
    FROM
        {{ ref('silver__decoded_logs') }}
        l
    WHERE
        block_number >= 12676663
        AND l.event_name = 'Transfer'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                all_swaps
            WHERE
                received_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                swap_eth_for_nft_from_vault_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
swap_nft_for_eth_from_vault_nft_price_agg AS (
    SELECT
        block_number,
        tx_hash,
        SUM(nft_tokens_created) AS total_nft_tokens_created,
        SUM(total_fees) AS total_fees_raw,
        SUM(platform_fees) AS total_platform_fees,
        SUM(weth_received) AS total_weth_received
    FROM
        swap_nft_for_eth_from_vault_nft_price
    GROUP BY
        block_number,
        tx_hash
),
swap_nft_for_eth_from_vault_nft_transfers AS (
    SELECT
        tx_hash,
        block_timestamp,
        to_address AS vault_address,
        from_address AS seller_address,
        to_address AS buyer_address,
        contract_address AS nft_address,
        tokenid,
        project_name,
        erc1155_value,
        token_metadata
    FROM
        {{ ref('silver__nft_transfers') }}
        t
        INNER JOIN vaults v
        ON t.to_address = v.vault_address
    WHERE
        block_timestamp >= '2021-06-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                swap_nft_for_eth_from_vault_nft_price
        )
        AND vault_address IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
swap_nft_for_eth_logs AS (
    SELECT
        tx_hash,
        event_index,
        contract_address AS vault_address_token_minted,
        'Minted' AS event_name,
        'sale' AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND contract_address IN (
            SELECT
                vault_address
            FROM
                vaults
        )
        AND topics [0] :: STRING = '0x1f72ad2a14447fa756b6f5aca53504645af79813493aca2d906b69e4aaeb9492'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
swap_nft_for_eth_from_vault_base AS (
    SELECT
        p.block_number,
        t.tx_hash,
        t.seller_address,
        t.buyer_address,
        t.nft_address,
        t.tokenid,
        p.total_nft_tokens_created,
        p.total_fees_raw,
        p.total_fees_raw / p.total_nft_tokens_created AS total_fees,
        p.total_platform_fees / p.total_nft_tokens_created AS platform_fee,
        total_fees - platform_fee AS creator_fee,
        p.total_weth_received / p.total_nft_tokens_created AS price_in_eth_per_nft,
        IFF(
            total_fees > 0,
            price_in_eth_per_nft * total_fees *(
                creator_fee / total_fees
            ),
            0
        ) AS creator_fee_in_eth_per_nft,
        IFF(
            total_fees > 0,
            price_in_eth_per_nft * total_fees *(
                platform_fee / total_fees
            ),
            0
        ) AS platform_fee_in_eth_per_nft,
        creator_fee_in_eth_per_nft + platform_fee_in_eth_per_nft AS total_fees_in_eth,
        t.project_name,
        t.erc1155_value,
        t.token_metadata,
        t.vault_address,
        l.event_index,
        l.event_name,
        l.event_type,
        l._log_id,
        l._inserted_timestamp
    FROM
        swap_nft_for_eth_from_vault_nft_transfers t
        INNER JOIN swap_nft_for_eth_from_vault_nft_price_agg p
        ON t.tx_hash = p.tx_hash
        INNER JOIN swap_nft_for_eth_logs l
        ON t.tx_hash = l.tx_hash
        AND t.vault_address = l.vault_address_token_minted
    WHERE
        total_nft_tokens_created > 0
        AND total_weth_received > 0
),
final_base AS (
    SELECT
        block_number,
        tx_hash,
        vault_address AS platform_address,
        event_index,
        event_name,
        event_type,
        seller_address,
        buyer_address,
        nft_address,
        tokenid,
        'ETH' AS currency_address,
        price_in_eth_per_nft AS price,
        total_fees_in_eth AS total_fees,
        platform_fee_in_eth_per_nft AS platform_fee,
        creator_fee_in_eth_per_nft AS creator_fee,
        _log_id,
        _inserted_timestamp
    FROM
        swap_nft_for_eth_from_vault_base
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        vault_address AS platform_address,
        event_index,
        event_name,
        event_type,
        seller_address,
        buyer_address,
        nft_address,
        tokenid,
        'ETH' AS currency_address,
        price_in_eth_per_nft AS price,
        total_fees_in_eth AS total_fees,
        platform_fee_in_eth AS platform_fee,
        creator_fee_in_eth AS creator_fee,
        _log_id,
        _inserted_timestamp
    FROM
        swap_eth_for_nft_from_vault_base
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        contract_address AS platform_address,
        event_index,
        event_name,
        event_type,
        seller_address,
        buyer_address,
        nft_address,
        tokenid,
        currency_address,
        -- the token address that is used to claim the nft // same as vault address
        price,
        total_fees,
        platform_fee,
        creator_fee,
        _log_id,
        _inserted_timestamp
    FROM
        redeem_txs_direct_vault_base
),
tx_data AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12676663
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                final_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address,
        tokenid,
        erc1155_value,
        project_name,
        token_metadata
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                final_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
all_prices AS (
    SELECT
        HOUR,
        CASE
            WHEN symbol IS NULL
            AND token_address IS NULL THEN 'ETH'
            ELSE symbol
        END AS symbol,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS currency_address,
        CASE
            WHEN currency_address = 'ETH' THEN '18'
            ELSE decimals
        END AS decimals,
        AVG(price) AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    final_base
            )
            OR (
                token_address IS NULL
                AND symbol IS NULL
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
        AND HOUR :: DATE >= '2021-06-01'
    GROUP BY
        HOUR,
        decimals,
        symbol,
        token_address
),
eth_price AS (
    SELECT
        HOUR,
        AVG(price) AS eth_price_hourly
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE >= '2021-06-01'
        AND token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
    GROUP BY
        HOUR
),
final_base_txs AS (
    SELECT
        b.tx_hash,
        b.platform_address,
        b.event_index,
        b.event_name,
        b.event_type,
        b.seller_address,
        b.buyer_address,
        b.nft_address,
        b.tokenid,
        b.currency_address,
        b.price,
        b.total_fees,
        b.platform_fee,
        b.creator_fee,
        _log_id,
        _inserted_timestamp,
        t.block_number,
        t.block_timestamp,
        t.from_address AS origin_from_address,
        t.to_address AS origin_to_address,
        t.origin_function_signature,
        t.tx_fee,
        t.input_data
    FROM
        final_base b
        INNER JOIN tx_data t
        ON b.block_number = t.block_number
        AND b.tx_hash = t.tx_hash
),
final_nftx AS (
    SELECT
        block_timestamp,
        block_number,
        b.tx_hash,
        platform_address,
        'nftx' AS platform_name,
        'nftx' AS platform_exchange_version,
        event_index,
        event_name,
        event_type,
        seller_address,
        buyer_address,
        b.nft_address,
        b.tokenid,
        n.project_name,
        n.erc1155_value,
        n.token_metadata,
        b.currency_address,
        -- for nft <-> eth swaps, currency is in ETH. For redeems, currency is the vault address
        ap.symbol AS currency_symbol,
        price,
        total_fees,
        platform_fee,
        creator_fee,
        COALESCE (
            price * hourly_prices,
            0
        ) AS price_usd,
        COALESCE (
            total_fees * hourly_prices,
            0
        ) AS total_fees_usd,
        COALESCE (
            platform_fee * hourly_prices,
            0
        ) AS platform_fee_usd,
        COALESCE (
            creator_fee * hourly_prices,
            0
        ) AS creator_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_fee,
        tx_fee * eth_price_hourly AS tx_fee_usd,
        input_data,
        CONCAT (
            _log_id,
            '-',
            b.nft_address,
            '-',
            b.tokenid
        ) AS log_id_nft,
        _inserted_timestamp
    FROM
        final_base_txs b
        LEFT JOIN nft_transfers n
        ON n.tx_hash = b.tx_hash
        AND n.contract_address = b.nft_address
        AND n.tokenId = b.tokenId
        LEFT JOIN all_prices ap
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = ap.hour
        AND b.currency_address = ap.currency_address
        LEFT JOIN eth_price ep
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = ep.hour qualify(ROW_NUMBER() over(PARTITION BY log_id_nft
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    *
FROM
    final_nftx
