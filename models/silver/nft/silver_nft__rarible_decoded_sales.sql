{{ config(
    materialized = 'incremental',
    unique_key = 'log_id_nft',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH rarible_treasury_wallets AS (

    SELECT
        *
    FROM
        (
            VALUES
                (
                    LOWER('0xe627243104A101Ca59a2c629AdbCd63a782E837f')
                ),
                ('0xb3dc72ada453547a3dec51867f4e1cce24d5d597'),
                ('0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a')
        ) t (address)
),
v1_base_logs AS (
    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        decoded_flat,
        decoded_flat :buyer :: STRING AS buyer_temp,
        decoded_flat :owner :: STRING AS seller_temp,
        decoded_flat :amount AS amount,
        decoded_flat :buyToken :: STRING AS buy_token,
        decoded_flat :buyTokenId :: STRING AS buy_tokenid,
        decoded_flat :buyValue AS buy_value,
        decoded_flat :sellToken :: STRING AS sell_token,
        decoded_flat :sellTokenId :: STRING AS sell_tokenid,
        decoded_flat :sellValue AS sell_value,
        CASE
            WHEN buy_token = '0x0000000000000000000000000000000000000000' THEN buyer_temp
            ELSE NULL
        END AS buyer_address_temp,
        CASE
            WHEN buy_token = '0x0000000000000000000000000000000000000000' THEN seller_temp
            ELSE NULL
        END AS seller_address_temp,
        CASE
            WHEN buy_token = '0x0000000000000000000000000000000000000000' THEN sell_token
            ELSE NULL
        END AS nft_address_temp,
        CASE
            WHEN buy_token = '0x0000000000000000000000000000000000000000' THEN sell_tokenid
            ELSE NULL
        END AS tokenId_temp,
        CASE
            WHEN buy_token = '0x0000000000000000000000000000000000000000' THEN buy_token
            ELSE NULL
        END AS currency_address_temp,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 11274515
        AND contract_address IN (
            '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
            -- exchange 1,
            '0x09eab21c40743b2364b94345419138ef80f39e30' -- exchange v1
        )
        AND event_name = 'Buy'

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
v1_payment_eth AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                eth_value DESC
        ) AS price_rank,
        CASE
            WHEN to_address IN (
                SELECT
                    address
                FROM
                    rarible_treasury_wallets
            ) THEN eth_value
            ELSE 0
        END AS treasury_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank = 1 THEN eth_value
            ELSE 0
        END AS price_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank != 1 THEN eth_value
            ELSE 0
        END AS royalty_label,
        eth_value
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_number >= 11274515
        AND identifier != 'CALL_ORIGIN'
        AND eth_value > 0
        AND from_address IN (
            '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
            '0x09eab21c40743b2364b94345419138ef80f39e30'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v1_base_logs
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
v1_payment_eth_agg AS (
    SELECT
        tx_hash,
        SUM(price_label) AS nft_price_eth,
        SUM(treasury_label) AS platform_fee_eth,
        SUM(royalty_label) AS creator_fee_eth
    FROM
        v1_payment_eth
    GROUP BY
        tx_hash
),
v1_base_eth AS (
    SELECT
        l.tx_hash,
        block_number,
        event_name,
        contract_address AS platform_address,
        decoded_flat,
        'sale' AS event_type,
        buyer_address_temp AS buyer_address,
        seller_address_temp AS seller_address,
        nft_address_temp AS nft_address,
        tokenId_temp AS tokenId,
        currency_address_temp AS currency_address,
        nft_price_eth + platform_fee_eth + creator_fee_eth AS total_price_eth,
        nft_price_eth,
        platform_fee_eth + creator_fee_eth AS total_fees_eth,
        platform_fee_eth,
        creator_fee_eth,
        _log_id,
        _inserted_timestamp
    FROM
        v1_base_logs l
        INNER JOIN v1_payment_eth_agg p
        ON l.tx_hash = p.tx_hash
),
v1_payment_erc20 AS (
    SELECT
        block_number,
        tx_hash,
        contract_address AS erc20_transferred,
        COALESCE (
            decoded_flat :src,
            decoded_flat :from
        ) :: STRING AS from_address,
        COALESCE (
            decoded_flat :dst,
            decoded_flat :to
        ) :: STRING AS to_address,
        COALESCE (
            decoded_flat :wad,
            decoded_flat :value,
            decoded_flat :tokens
        ) :: FLOAT AS amount_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            from_address,
            erc20_transferred
            ORDER BY
                amount_raw DESC,
                event_index DESC
        ) AS price_rank,
        CASE
            WHEN to_address IN (
                SELECT
                    address
                FROM
                    rarible_treasury_wallets
            ) THEN amount_raw
            ELSE 0
        END AS treasury_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank = 1 THEN amount_raw
            ELSE 0
        END AS price_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank != 1 THEN amount_raw
            ELSE 0
        END AS royalty_label
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 11274515
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v1_base_logs
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v1_payment_eth
        )
        AND event_name = 'Transfer'
        AND decoded_flat :tokenId IS NULL
        AND amount_raw IS NOT NULL
        AND from_address IS NOT NULL
        AND to_address IS NOT NULL

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
v1_payment_erc20_agg AS (
    SELECT
        tx_hash,
        from_address,
        erc20_transferred,
        SUM(price_label) AS nft_price_erc20,
        SUM(treasury_label) AS platform_fee_erc20,
        SUM(royalty_label) AS creator_fee_erc20
    FROM
        v1_payment_erc20
    GROUP BY
        tx_hash,
        from_address,
        erc20_transferred
),
v1_buyer_address AS (
    SELECT
        tx_hash,
        from_address AS buyer_address_from_erc20base
    FROM
        v1_payment_erc20
    WHERE
        price_rank = 1 qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                amount_raw DESC
        ) = 1
),
v1_base_erc20 AS (
    SELECT
        l.tx_hash,
        block_number,
        event_name,
        contract_address AS platform_address,
        decoded_flat,
        CASE
            WHEN buyer_temp = buyer_address_from_erc20base
            AND seller_temp = buyer_address_from_erc20base THEN 'bid_won'
            WHEN buyer_temp = buyer_address_from_erc20base
            AND seller_temp != buyer_address_from_erc20base THEN 'sale'
            WHEN seller_temp = buyer_address_from_erc20base
            AND buyer_temp != buyer_address_from_erc20base THEN 'bid_won'
        END AS event_type,
        CASE
            WHEN event_type = 'sale' THEN buyer_temp
            ELSE seller_temp
        END AS buyer_address,
        CASE
            WHEN event_type = 'sale' THEN seller_temp
            ELSE buyer_temp
        END AS seller_address,
        CASE
            WHEN event_type = 'sale' THEN sell_token
            ELSE buy_token
        END AS nft_address,
        CASE
            WHEN event_type = 'sale' THEN sell_tokenid
            ELSE buy_tokenid
        END AS tokenId,
        CASE
            WHEN event_type = 'sale' THEN buy_token
            ELSE sell_token
        END AS currency_address,
        _log_id,
        _inserted_timestamp
    FROM
        v1_base_logs l
        INNER JOIN v1_buyer_address b
        ON l.tx_hash = b.tx_hash
),
v1_base_erc20_with_amount AS (
    SELECT
        l.tx_hash,
        block_number,
        event_name,
        platform_address,
        decoded_flat,
        event_type,
        buyer_address,
        seller_address,
        nft_address,
        tokenId,
        currency_address,
        nft_price_erc20 + platform_fee_erc20 + creator_fee_erc20 AS total_price_erc20,
        nft_price_erc20,
        platform_fee_erc20 + creator_fee_erc20 AS total_fees_erc20,
        platform_fee_erc20,
        creator_fee_erc20,
        _log_id,
        _inserted_timestamp
    FROM
        v1_base_erc20 l
        INNER JOIN v1_payment_erc20_agg p
        ON l.tx_hash = p.tx_hash
        AND l.buyer_address = p.from_address
        AND l.currency_address = p.erc20_transferred
),
v1_base_zero_eth AS (
    SELECT
        tx_hash,
        block_number,
        event_name,
        contract_address AS platform_address,
        decoded_flat,
        'sale' AS event_type,
        buyer_address_temp AS buyer_address,
        seller_address_temp AS seller_address,
        nft_address_temp AS nft_address,
        tokenId_temp AS tokenId,
        currency_address_temp AS currency_address,
        0 AS total_price_eth,
        0 AS nft_price_eth,
        0 AS total_fees_eth,
        0 AS platform_fee_eth,
        0 AS creator_fee_eth,
        _log_id,
        _inserted_timestamp
    FROM
        v1_base_logs
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v1_base_eth
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v1_base_erc20_with_amount
        )
),
v1_base_combined AS (
    SELECT
        *
    FROM
        v1_base_eth
    UNION ALL
    SELECT
        *
    FROM
        v1_base_erc20_with_amount
    UNION ALL
    SELECT
        *
    FROM
        v1_base_zero_eth
),
v2_all_tx AS (
    SELECT
        tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND contract_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'

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
v2_multi_eth_tx AS (
    SELECT
        *
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_all_tx
        )
        AND identifier != 'CALL_ORIGIN'
        AND eth_value > 0
        AND to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'

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
v2_single_eth_tx AS (
    SELECT
        *
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_all_tx
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v2_multi_eth_tx
        )
        AND identifier != 'CALL_ORIGIN'
        AND eth_value > 0
        AND from_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND to_address != '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'

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
v2_single_eth_payment AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                eth_value DESC
        ) AS price_rank,
        CASE
            WHEN to_address IN (
                SELECT
                    address
                FROM
                    rarible_treasury_wallets
            ) THEN eth_value
            ELSE 0
        END AS treasury_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank = 1 THEN eth_value
            ELSE 0
        END AS price_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank != 1 THEN eth_value
            ELSE 0
        END AS royalty_label,
        eth_value
    FROM
        v2_single_eth_tx
),
v2_single_eth_seller_address AS (
    SELECT
        tx_hash,
        to_address AS seller_address_from_single_eth
    FROM
        v2_single_eth_payment
    WHERE
        price_rank = 1 qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                eth_value DESC
        ) = 1
),
v2_single_eth_payment_agg AS (
    SELECT
        tx_hash,
        SUM(price_label) AS nft_price_eth,
        SUM(treasury_label) AS platform_fee_eth,
        SUM(royalty_label) AS creator_fee_eth
    FROM
        v2_single_eth_payment
    GROUP BY
        tx_hash
),
v2_nft_transfers AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        from_address,
        to_address,
        contract_address AS nft_address,
        tokenid,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_single_eth_payment_agg
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            contract_address,
            tokenid
            ORDER BY
                event_index DESC
        ) = 1

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
v2_single_eth_base AS (
    SELECT
        block_timestamp,
        block_number,
        p.tx_hash,
        from_address,
        to_address,
        seller_address_from_single_eth,
        CASE
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN seller_address_from_single_eth
            WHEN from_address = seller_address_from_single_eth THEN from_address
            ELSE to_address
        END AS seller_address,
        CASE
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN to_address
            WHEN from_address = seller_address THEN to_address
            ELSE from_address
        END AS buyer_address,
        CASE
            WHEN seller_address_from_single_eth = from_address THEN 'sale'
            ELSE 'bid_won'
        END AS event_type,
        nft_address,
        tokenid,
        nft_price_eth,
        platform_fee_eth,
        creator_fee_eth,
        platform_fee_eth + creator_fee_eth AS total_fees_eth,
        nft_price_eth + total_fees_eth AS total_price_eth,
        _log_id,
        _inserted_timestamp
    FROM
        v2_nft_transfers t
        INNER JOIN v2_single_eth_payment_agg p
        ON t.tx_hash = p.tx_hash
        INNER JOIN v2_single_eth_seller_address s
        ON t.tx_hash = s.tx_hash
),
v2_multi_eth_payment AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        CASE
            WHEN to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6' THEN 1
            ELSE 0
        END AS initial_mark,
        SUM(initial_mark) over (
            PARTITION BY tx_hash
            ORDER BY
                identifier ASC
        ) AS purchase_order,
        -- order by gas descending
        gas,
        eth_value
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_multi_eth_tx
        )
        AND identifier != 'CALL_ORIGIN'
        AND eth_value > 0
        AND (
            to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
            OR from_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
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
v2_multi_eth_payment_labels AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            purchase_order
            ORDER BY
                eth_value DESC
        ) AS price_rank,
        CASE
            WHEN to_address IN (
                SELECT
                    address
                FROM
                    rarible_treasury_wallets
            ) THEN eth_value
            ELSE 0
        END AS treasury_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank = 1 THEN eth_value
            ELSE 0
        END AS price_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank != 1 THEN eth_value
            ELSE 0
        END AS royalty_label
    FROM
        v2_multi_eth_payment
    WHERE
        to_address != '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
),
v2_multi_eth_payment_agg AS (
    SELECT
        tx_hash,
        purchase_order,
        SUM(price_label) AS nft_price_eth,
        SUM(treasury_label) AS platform_fee_eth,
        SUM(royalty_label) AS creator_fee_eth
    FROM
        v2_multi_eth_payment_labels
    GROUP BY
        tx_hash,
        purchase_order
),
v2_multi_eth_seller AS (
    SELECT
        tx_hash,
        purchase_order,
        to_address AS seller_address_from_payment
    FROM
        v2_multi_eth_payment_labels
    WHERE
        price_rank = 1
),
v2_multi_eth_nft_transfers_order AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        contract_address AS nft_address,
        tokenid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS nft_transfer_rank,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_multi_eth_tx
        )
        AND from_address != '0x0000000000000000000000000000000000000000'
        AND to_address != '0x0000000000000000000000000000000000000000'

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
v2_multi_eth_base AS (
    SELECT
        block_timestamp,
        block_number,
        t.tx_hash,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        nft_transfer_rank,
        s.purchase_order,
        seller_address_from_payment,
        _log_id,
        _inserted_timestamp
    FROM
        v2_multi_eth_nft_transfers_order t
        INNER JOIN v2_multi_eth_seller s
        ON t.tx_hash = s.tx_hash
        AND nft_from_address = seller_address_from_payment qualify ROW_NUMBER() over (
            PARTITION BY t.tx_hash,
            nft_to_address,
            nft_address,
            tokenid,
            nft_transfer_rank
            ORDER BY
                block_timestamp ASC
        ) = 1
),
v2_multi_eth_base_with_payment AS (
    SELECT
        block_timestamp,
        block_number,
        t.tx_hash,
        nft_from_address AS seller_address,
        nft_to_address AS buyer_address,
        nft_address,
        tokenid,
        nft_price_eth,
        platform_fee_eth,
        creator_fee_eth,
        platform_fee_eth + creator_fee_eth AS total_fees_eth,
        nft_price_eth + total_fees_eth AS total_price_eth,
        nft_transfer_rank,
        t.purchase_order,
        seller_address_from_payment,
        _log_id,
        _inserted_timestamp
    FROM
        v2_multi_eth_base t
        INNER JOIN v2_multi_eth_payment_agg p
        ON t.tx_hash = p.tx_hash
        AND t.purchase_order = p.purchase_order
),
v2_erc20_payment AS (
    SELECT
        block_number,
        tx_hash,
        contract_address AS erc20_transferred,
        COALESCE (
            decoded_flat :src,
            decoded_flat :from
        ) :: STRING AS from_address,
        COALESCE (
            decoded_flat :dst,
            decoded_flat :to
        ) :: STRING AS to_address,
        COALESCE (
            decoded_flat :wad,
            decoded_flat :value
        ) :: FLOAT AS amount_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            from_address,
            erc20_transferred
            ORDER BY
                amount_raw DESC,
                event_index DESC
        ) AS price_rank,
        CASE
            WHEN to_address IN (
                SELECT
                    address
                FROM
                    rarible_treasury_wallets
            ) THEN amount_raw
            ELSE 0
        END AS treasury_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank = 1 THEN amount_raw
            ELSE 0
        END AS price_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank != 1 THEN amount_raw
            ELSE 0
        END AS royalty_label
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_all_tx
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v2_multi_eth_tx
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v2_single_eth_tx
        )
        AND event_name = 'Transfer'
        AND decoded_flat :tokenId IS NULL
        AND decoded_flat :id IS NULL
        AND amount_raw IS NOT NULL
        AND from_address != '0x0000000000000000000000000000000000000000'
        AND to_address != '0x0000000000000000000000000000000000000000'

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
v2_erc20_buyer_seller_list AS (
    SELECT
        tx_hash,
        erc20_transferred,
        from_address AS buyer,
        to_address AS seller,
        amount_raw,
        CONCAT(
            tx_hash,
            '-',
            buyer,
            '-',
            seller
        ) AS tx_hash_erc20_identifier
    FROM
        v2_erc20_payment
    WHERE
        price_rank = 1
),
v2_erc20_nft_transfers AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        contract_address AS nft_address,
        tokenid,
        CONCAT(
            tx_hash,
            '-',
            nft_to_address,
            '-',
            nft_from_address
        ) AS tx_hash_nft_identifier,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS nft_transfer_rank,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp >= '2021-06-01'
        AND block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_erc20_payment
        )
        AND from_address != '0x0000000000000000000000000000000000000000'
        AND to_address != '0x0000000000000000000000000000000000000000'

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
v2_erc20_nft_transfer_filters AS (
    SELECT
        block_timestamp,
        block_number,
        l.tx_hash,
        erc20_transferred,
        buyer,
        seller,
        amount_raw,
        nft_from_address,
        nft_to_address,
        nft_address,
        tokenid,
        nft_transfer_rank,
        _log_id,
        _inserted_timestamp
    FROM
        v2_erc20_buyer_seller_list l
        INNER JOIN v2_erc20_nft_transfers t
        ON l.tx_hash = t.tx_hash
        AND tx_hash_erc20_identifier = tx_hash_nft_identifier
),
v2_erc20_payment_agg AS (
    SELECT
        tx_hash,
        from_address,
        erc20_transferred,
        SUM(price_label) AS nft_price_erc20,
        SUM(treasury_label) AS platform_fee_erc20,
        SUM(royalty_label) AS creator_fee_erc20
    FROM
        v2_erc20_payment
    GROUP BY
        tx_hash,
        from_address,
        erc20_transferred
),
v2_erc20_base AS (
    SELECT
        block_timestamp,
        block_number,
        t.tx_hash,
        buyer AS buyer_address,
        seller AS seller_address,
        nft_address,
        tokenid,
        nft_transfer_rank,
        t.erc20_transferred,
        nft_price_erc20,
        platform_fee_erc20,
        creator_fee_erc20,
        platform_fee_erc20 + creator_fee_erc20 AS total_fees_erc20,
        nft_price_erc20 + total_fees_erc20 AS total_price_erc20,
        _log_id,
        _inserted_timestamp
    FROM
        v2_erc20_nft_transfer_filters t
        INNER JOIN v2_erc20_payment_agg p
        ON t.tx_hash = p.tx_hash
        AND t.buyer = p.from_address
        AND t.erc20_transferred = p.erc20_transferred
),
v2_base AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        seller_address,
        buyer_address,
        event_type,
        nft_address,
        tokenid,
        'ETH' AS currency_address,
        total_price_eth AS price_raw,
        total_fees_eth AS total_fees_raw,
        platform_fee_eth AS platform_fee_raw,
        creator_fee_eth AS creator_fee_raw,
        _log_id,
        _inserted_timestamp
    FROM
        v2_single_eth_base
    UNION ALL
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        seller_address,
        buyer_address,
        NULL AS event_type,
        nft_address,
        tokenid,
        'ETH' AS currency_address,
        total_price_eth,
        total_fees_eth,
        platform_fee_eth,
        creator_fee_eth,
        _log_id,
        _inserted_timestamp
    FROM
        v2_multi_eth_base_with_payment
    UNION ALL
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        seller_address,
        buyer_address,
        NULL AS event_type,
        nft_address,
        tokenid,
        erc20_transferred AS currency_address,
        total_price_erc20,
        total_fees_erc20,
        platform_fee_erc20,
        creator_fee_erc20,
        _log_id,
        _inserted_timestamp
    FROM
        v2_erc20_base
),
v2_all_tx_event_type AS (
    SELECT
        tx_hash,
        CASE
            WHEN decoded_flat :newLeftFill > 1 THEN 'sale'
            ELSE 'bid_won'
        END AS event_type
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 12617828
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v2_all_tx
        )
        AND contract_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) = 1

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
v2_base_final AS (
    SELECT
        block_number,
        block_timestamp,
        b.tx_hash,
        COALESCE (
            b.event_type,
            t.event_type,
            'bid_won'
        ) AS event_type,
        '0x9757f2d2b135150bbeb65308d4a91804107cd8d6' AS platform_address,
        'rarible' AS platform_name,
        'rarible v2' AS platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        tokenid,
        currency_address,
        price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        _log_id,
        _inserted_timestamp
    FROM
        v2_base b
        LEFT JOIN v2_all_tx_event_type t
        ON b.tx_hash = t.tx_hash
),
tx_data AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp >= '2020-11-01'
        AND block_number >= 11274515

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
v2_base_final_tx AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        tokenid,
        currency_address,
        price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        _log_id,
        _inserted_timestamp
    FROM
        v2_base_final b
        INNER JOIN tx_data t
        ON b.tx_hash = t.tx_hash
),
v1_base_final_tx AS (
    SELECT
        b.block_number,
        t.block_timestamp,
        b.tx_hash,
        event_type,
        platform_address,
        'rarible' AS platform_name,
        'rarible v1' AS platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        CASE
            WHEN currency_address = '0x0000000000000000000000000000000000000000' THEN 'ETH'
            ELSE currency_address
        END AS currency_address,
        total_price_eth AS price_raw,
        total_fees_eth AS total_fees_raw,
        platform_fee_eth AS platform_fee_raw,
        creator_fee_eth AS creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        _log_id,
        _inserted_timestamp
    FROM
        v1_base_combined b
        INNER JOIN tx_data t
        ON b.tx_hash = t.tx_hash
),
v1_v2_base_combined AS (
    SELECT
        *
    FROM
        v1_base_final_tx
    UNION ALL
    SELECT
        *
    FROM
        v2_base_final_tx
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
        block_timestamp >= '2020-11-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v1_v2_base_combined
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
        symbol,
        token_address AS currency_address,
        decimals,
        (price) AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    v1_v2_base_combined
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                tx_data
        )
        AND HOUR :: DATE >= '2020-11-01'
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS currency_address,
        1 AS decimals,
        (price) AS hourly_prices
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                v1_v2_base_combined
        )
        AND HOUR :: DATE >= '2020-11-01'
),
eth_price AS (
    SELECT
        HOUR,
        (price) AS eth_price_hourly
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE >= '2020-11-01'
        AND token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                v1_v2_base_combined
        )
)
SELECT
    block_number,
    block_timestamp,
    b.tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    b.nft_address,
    b.tokenid,
    n.project_name,
    n.erc1155_value,
    n.token_metadata,
    b.currency_address,
    ap.symbol AS currency_symbol,
    CASE
        WHEN b.currency_address = 'ETH' THEN price_raw
        WHEN b.currency_address IN (
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN price_raw / pow(
            10,
            18
        )
        ELSE COALESCE (price_raw / pow(10, decimals), price_raw)
    END AS price,
    CASE
        WHEN b.currency_address = 'ETH' THEN price_raw
        WHEN b.currency_address IN (
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN total_fees_raw / pow(
            10,
            18
        )
        ELSE COALESCE (total_fees_raw / pow(10, decimals), total_fees_raw)
    END AS total_fees,
    CASE
        WHEN b.currency_address = 'ETH' THEN price_raw
        WHEN b.currency_address IN (
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN platform_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (platform_fee_raw / pow(10, decimals), platform_fee_raw)
    END AS platform_fee,
    CASE
        WHEN b.currency_address = 'ETH' THEN price_raw
        WHEN b.currency_address IN (
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN creator_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (creator_fee_raw / pow(10, decimals), creator_fee_raw)
    END AS creator_fee,
    IFF(
        decimals IS NULL,
        0,
        price * hourly_prices
    ) AS price_usd,
    IFF(
        decimals IS NULL,
        0,
        total_fees * hourly_prices
    ) AS total_fees_usd,
    IFF(
        decimals IS NULL,
        0,
        platform_fee * hourly_prices
    ) AS platform_fee_usd,
    IFF(
        decimals IS NULL,
        0,
        creator_fee * hourly_prices
    ) AS creator_fee_usd,
    tx_fee,
    tx_fee * eth_price_hourly AS tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        _log_id,
        '-',
        b.nft_address,
        '-',
        b.tokenid
    ) AS log_id_nft
FROM
    v1_v2_base_combined b
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
    ) = ep.hour qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1