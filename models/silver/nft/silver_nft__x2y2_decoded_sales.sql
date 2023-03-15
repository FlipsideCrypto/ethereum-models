{{ config(
    materialized = 'incremental',
    unique_key = 'log_id_nft',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH x2y2_fee_address AS (

    SELECT
        '0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd' AS address
),
ev_inventory_base AS (
    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        CASE
            WHEN decoded_flat :currency :: STRING = '0x0000000000000000000000000000000000000000' THEN 'ETH'
            ELSE decoded_flat :currency :: STRING
        END AS currency_address,
        decoded_flat :delegateType :: INT AS sale_type,
        -- when it's 2, maker is the order creator, taker is order accepter. nft buyer = order creator
        -- when it's 1, maker is nft seller, taker = nft buyer
        decoded_flat :maker :: STRING AS maker,
        decoded_flat :taker :: STRING AS taker,
        CASE
            WHEN sale_type = 1 THEN maker
            WHEN sale_type = 2 THEN taker
            ELSE NULL
        END AS seller_address,
        CASE
            WHEN sale_type = 1 THEN taker
            WHEN sale_type = 2 THEN maker
            ELSE NULL
        END AS buyer_address,
        CASE
            WHEN sale_type = 1 THEN 'sale'
            WHEN sale_type = 2 THEN 'bid_won'
            ELSE NULL
        END AS event_type,
        TRY_BASE64_DECODE_BINARY (
            decoded_flat :item [1] :: STRING
        ) :: STRING AS item_decoded,
        LOWER(CONCAT('0x', SUBSTR(item_decoded, 153, 40))) :: STRING AS nft_address,
        (
            ethereum.public.udf_hex_to_int(SUBSTR(item_decoded, 193, 64))
        ) :: STRING AS tokenId,
        (
            ethereum.public.udf_hex_to_int(SUBSTR(item_decoded, 257, 64))
        ) :: STRING AS tokenId_quantity,
        decoded_flat :detail [3] :: INT AS price_raw,
        decoded_flat :detail [10] AS fee_details,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 14139341
        AND contract_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
        AND event_name = 'EvInventory'

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
fees AS (
    SELECT
        tx_hash,
        event_index,
        price_raw,
        VALUE [0] :: INT / pow(
            10,
            6
        ) AS percent,
        percent * price_raw AS fee_amount_raw,
        VALUE [1] :: STRING AS receiver_address
    FROM
        ev_inventory_base,
        LATERAL FLATTEN(
            input => fee_details
        )
),
fees_agg AS (
    SELECT
        tx_hash,
        event_index,
        SUM (
            CASE
                WHEN receiver_address IN (
                    SELECT
                        address
                    FROM
                        x2y2_fee_address
                ) THEN fee_amount_raw
                ELSE 0
            END
        ) AS platform_fee_raw_,
        SUM(
            CASE
                WHEN receiver_address NOT IN (
                    SELECT
                        address
                    FROM
                        x2y2_fee_address
                ) THEN fee_amount_raw
                ELSE 0
            END
        ) AS creator_fee_raw_
    FROM
        fees
    GROUP BY
        tx_hash,
        event_index
),
nft_details AS (
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
        block_timestamp >= '2022-01-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                ev_inventory_base
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
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        from_address,
        to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
        AND block_number >= 14139341
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                ev_inventory_base
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
                    ev_inventory_base
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
        AND HOUR :: DATE >= '2022-01-01'
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
        HOUR :: DATE >= '2022-01-01'
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
base_sales AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        event_name,
        'x2y2' AS platform_name,
        'x2y2' AS platform_exchange_version,
        b.contract_address AS platform_address,
        b.currency_address,
        sale_type,
        seller_address,
        buyer_address,
        event_type,
        price_raw,
        COALESCE (
            platform_fee_raw_,
            0
        ) AS platform_fee_raw,
        COALESCE (
            creator_fee_raw_,
            0
        ) AS creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        b.nft_address,
        b.tokenId,
        b.tokenId_quantity,
        n.erc1155_value,
        n.token_metadata,
        n.project_name,
        _log_id,
        CONCAT(
            _log_id,
            '-',
            b.nft_address,
            '-',
            b.tokenId
        ) AS log_id_nft,
        _inserted_timestamp
    FROM
        ev_inventory_base b
        LEFT JOIN fees_agg f
        ON b.tx_hash = f.tx_hash
        AND b.event_index = f.event_index
        LEFT JOIN nft_details n
        ON b.tx_hash = b.tx_hash
        AND b.nft_address = n.contract_address
        AND b.tokenId = n.tokenId
        AND b.tokenId_quantity = n.erc1155_value qualify(ROW_NUMBER() over(PARTITION BY log_id_nft
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    b.tx_hash,
    b.block_number,
    t.block_timestamp,
    event_index,
    event_name,
    platform_name,
    platform_exchange_version,
    platform_address,
    b.currency_address,
    ap.symbol AS currency_symbol,
    sale_type,
    seller_address,
    buyer_address,
    event_type,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN price_raw / pow(
            10,
            18
        )
        ELSE COALESCE (price_raw / pow(10, decimals), 0)
    END AS price,
    COALESCE (
        price * hourly_prices,
        0
    ) AS price_usd,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN total_fees_raw / pow(
            10,
            18
        )
        ELSE COALESCE (total_fees_raw / pow(10, decimals), 0)
    END AS total_fees,
    COALESCE (
        total_fees * hourly_prices,
        0
    ) AS total_fees_usd,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN platform_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (platform_fee_raw / pow(10, decimals), 0)
    END AS platform_fee,
    COALESCE (
        platform_fee * hourly_prices,
        0
    ) AS platform_fee_usd,
    CASE
        WHEN b.currency_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) THEN creator_fee_raw / pow(
            10,
            18
        )
        ELSE COALESCE (creator_fee_raw / pow(10, decimals), 0)
    END AS creator_fee,
    COALESCE (
        creator_fee * hourly_prices,
        0
    ) AS creator_fee_usd,
    nft_address,
    tokenId,
    erc1155_value,
    token_metadata,
    project_name,
    from_address AS origin_from_address,
    to_address AS origin_to_address,
    origin_function_signature,
    tx_fee,
    tx_fee * eth_price_hourly AS tx_fee_usd,
    input_data,
    _log_id,
    log_id_nft,
    _inserted_timestamp
FROM
    base_sales b
    INNER JOIN tx_data t
    ON b.tx_hash = t.tx_hash
    LEFT JOIN eth_price ep
    ON DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = ep.hour
    LEFT JOIN all_prices ap
    ON DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = ap.hour
    AND b.currency_address = ap.currency_address
