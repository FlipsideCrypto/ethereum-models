{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
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
raw_decoded_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name,
        full_decoded_log,
        full_decoded_log AS decoded_data,
        decoded_log,
        decoded_log AS decoded_flat,
        contract_name,
        ez_decoded_event_logs_id,
        inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_number >= 11274515

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
v1_base_logs AS (
    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        decoded_log AS decoded_flat,
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
        raw_decoded_logs
    WHERE
        contract_address IN (
            '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
            -- exchange 1,
            '0x09eab21c40743b2364b94345419138ef80f39e30' -- exchange v1
        )
        AND event_name = 'Buy'
),
raw_traces AS (
    SELECT
        *
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_number >= 11274515
        AND concat_ws(
            '_',
            TYPE,
            trace_address
        ) != 'CALL_ORIGIN'
        AND value > 0
        AND TYPE = 'CALL'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
                value DESC
        ) AS price_rank,
        CASE
            WHEN to_address IN (
                SELECT
                    address
                FROM
                    rarible_treasury_wallets
            ) THEN value
            ELSE 0
        END AS treasury_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank = 1 THEN value
            ELSE 0
        END AS price_label,
        CASE
            WHEN treasury_label = 0
            AND price_rank != 1 THEN value
            ELSE 0
        END AS royalty_label,
        value AS eth_value
    FROM
        raw_traces
    WHERE
        from_address IN (
            '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
            '0x09eab21c40743b2364b94345419138ef80f39e30'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                v1_base_logs
        )
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
        event_index,
        -- new
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
        raw_decoded_logs
    WHERE
        tx_hash IN (
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
        event_index,
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
        event_index,
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
        event_index,
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
raw_nft_transfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp >= '2020-11-01'

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
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp >= '2020-11-01'
        AND block_number >= 11274515

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
v1_base_final_tx AS (
    SELECT
        b.block_number,
        t.block_timestamp,
        b.tx_hash,
        event_index,
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
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address,
        tokenid,
        erc1155_value
    FROM
        raw_nft_transfers
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                v1_base_final_tx
        )
)
SELECT
    block_number,
    block_timestamp,
    b.tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    b.nft_address,
    b.tokenid,
    n.erc1155_value,
    b.currency_address,
    CASE
        WHEN b.currency_address = 'ETH' THEN price_raw * pow(
            10,
            18
        )
        ELSE price_raw
    END AS total_price_raw,
    CASE
        WHEN b.currency_address = 'ETH' THEN total_fees_raw * pow(
            10,
            18
        )
        ELSE total_fees_raw
    END AS total_fees_raw,
    CASE
        WHEN b.currency_address = 'ETH' THEN platform_fee_raw * pow(
            10,
            18
        )
        ELSE platform_fee_raw
    END AS platform_fee_raw,
    CASE
        WHEN b.currency_address = 'ETH' THEN creator_fee_raw * pow(
            10,
            18
        )
        ELSE creator_fee_raw
    END AS creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        b.nft_address,
        '-',
        b.tokenid,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id
FROM
    v1_base_final_tx b
    LEFT JOIN nft_transfers n
    ON n.tx_hash = b.tx_hash
    AND n.contract_address = b.nft_address
    AND n.tokenId = b.tokenId qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
