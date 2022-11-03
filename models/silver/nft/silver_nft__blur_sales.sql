{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH event_base AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        ARRAY_SIZE(segmented_data) AS SIZE,
        CASE
            WHEN SIZE = 36 THEN CONCAT('0x', SUBSTR(segmented_data [7] :: STRING, 25, 40))
            WHEN SIZE = 34 THEN CONCAT('0x', SUBSTR(segmented_data [7] :: STRING, 25, 40))
        END AS nft_address,
        CASE
            WHEN SIZE = 36 THEN PUBLIC.udf_hex_to_int(
                segmented_data [8] :: STRING
            )
            WHEN SIZE = 34 THEN PUBLIC.udf_hex_to_int(
                segmented_data [8] :: STRING
            )
        END AS token_id,
        CASE
            WHEN SIZE = 36 THEN CONCAT(
                '0x',
                SUBSTR(
                    segmented_data [19] :: STRING,
                    25,
                    40
                )
            )
        END AS royalty_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS seller_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS buyer_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [11] :: STRING
        ) / pow(
            10,
            18
        ) AS sale_price_total
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-08-01'
        AND topics [0] :: STRING = '0x61cbb2a3dee0b6064c2e681aadd61677fb4ef319f0b547508d495626f5a62f64'
        AND contract_address = '0x000000000000ad05ccc4f10045630fb830b95127'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
royalty_purchases AS (
    SELECT
        tx_hash,
        event_index,
        royalty_address,
        ROW_NUMBER() over(
            PARTITION BY tx_hash,
            royalty_address
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        event_base
    WHERE
        royalty_address IS NOT NULL
),
royalty_addresses AS (
    SELECT
        DISTINCT tx_hash,
        royalty_address
    FROM
        event_base
    WHERE
        royalty_address IS NOT NULL
),
payments AS (
    SELECT
        tx_hash,
        to_address,
        eth_value,
        identifier,
        SPLIT(
            identifier,
            '_'
        ) AS split_id,
        split_id [1] :: INTEGER AS level1,
        split_id [2] :: INTEGER AS level2,
        split_id [3] :: INTEGER AS level3,
        split_id [4] :: INTEGER AS level4,
        split_id [5] :: INTEGER AS level5,
        split_id [6] :: INTEGER AS level6,
        split_id [7] :: INTEGER AS level7,
        split_id [8] :: INTEGER AS level8,
        split_id [9] :: INTEGER AS level9,
        split_id [10] :: INTEGER AS level10,
        split_id [11] :: INTEGER AS level11,
        split_id [12] :: INTEGER AS level12,
        split_id [13] :: INTEGER AS level13,
        split_id [14] :: INTEGER AS level14,
        split_id [15] :: INTEGER AS level15,
        split_id [16] :: INTEGER AS level16,
        split_id [17] :: INTEGER AS level17,
        split_id [18] :: INTEGER AS level18,
        split_id [19] :: INTEGER AS level19,
        split_id [20] :: INTEGER AS level20,
        ROW_NUMBER() over(
            PARTITION BY tx_hash,
            to_address
            ORDER BY
                level1 ASC,
                level2 ASC,
                level3 ASC,
                level4 ASC,
                level5 ASC,
                level6 ASC,
                level7 ASC,
                level8 ASC,
                level9 ASC,
                level10 ASC,
                level11 ASC,
                level12 ASC,
                level13 ASC,
                level14 ASC,
                level15 ASC,
                level16 ASC,
                level17 ASC,
                level18 ASC,
                level19 ASC,
                level20 ASC
        ) AS agg_id
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE >= '2022-08-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                event_base
        )
        AND eth_value > 0
        AND (
            from_address <> '0x39da41747a83aee658334415666f3ef92dd0d541'
            AND to_address <> '0x000000000000ad05ccc4f10045630fb830b95127'
        )
        AND identifier <> 'CALL_ORIGIN'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
royalty_payments AS (
    SELECT
        payments.tx_hash,
        to_address,
        eth_value,
        identifier,
        agg_id
    FROM
        payments
        INNER JOIN royalty_addresses
        ON payments.tx_hash = royalty_addresses.tx_hash
        AND payments.to_address = royalty_addresses.royalty_address
),
tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-08-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                event_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        tx_hash,
        event_index,
        contract_address AS nft_address,
        project_name,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        tokenId AS token_id,
        erc1155_value,
        token_metadata,
        CONCAT(
            tx_hash,
            '-',
            contract_address,
            '-',
            tokenId
        ) AS nft_id
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-08-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                event_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
nft_sale AS (
    SELECT
        tx_hash,
        event_index,
        nft_address,
        nft_from_address AS seller_address,
        token_id,
        erc1155_value,
        token_metadata,
        project_name
    FROM
        nft_transfers qualify(ROW_NUMBER() over(PARTITION BY nft_id
    ORDER BY
        event_index ASC)) = 1
),
nft_buy AS (
    SELECT
        tx_hash,
        event_index,
        nft_address,
        nft_to_address AS buyer_address,
        token_id,
        erc1155_value,
        token_metadata,
        project_name
    FROM
        nft_transfers qualify(ROW_NUMBER() over(PARTITION BY nft_id
    ORDER BY
        event_index DESC)) = 1
),
eth_prices AS (
    SELECT
        HOUR,
        AVG(price) AS eth_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR >= '2022-08-01'
    GROUP BY
        HOUR
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        t.origin_to_address,
        t.origin_from_address,
        t.origin_function_signature,
        'sale' AS event_type,
        b.contract_address AS platform_address,
        'blur' AS platform_name,
        'v1' AS platform_exchange_version,
        nb.buyer_address,
        ns.seller_address,
        ns.nft_address,
        ns.project_name,
        ns.erc1155_value,
        ns.token_id AS tokenId,
        ns.token_metadata,
        'ETH' AS currency_symbol,
        'ETH' AS currency_address,
        sale_price_total AS price,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        ROUND(
            sale_price_total * eth_price,
            2
        ) AS price_usd,
        COALESCE(
            eth_value,
            0
        ) AS total_fees,
        0 AS platform_fee,
        0 AS platform_fee_usd,
        COALESCE(
            eth_value,
            0
        ) AS creator_fee,
        ROUND(
            creator_fee * eth_price,
            2
        ) AS creator_fee_usd,
        ROUND(
            creator_fee * eth_price,
            2
        ) AS total_fees_usd,
        b._log_id,
        b._inserted_timestamp,
        input_data,
        tx_fee
    FROM
        event_base b
        LEFT JOIN tx_data t
        ON b.tx_hash = t.tx_hash
        LEFT JOIN nft_sale ns
        ON b.tx_hash = ns.tx_hash
        AND b.token_id = ns.token_id
        AND b.nft_address = ns.nft_address
        LEFT JOIN nft_buy nb
        ON b.tx_hash = nb.tx_hash
        AND b.token_id = nb.token_id
        AND b.nft_address = nb.nft_address
        LEFT JOIN eth_prices ep
        ON ep.hour = DATE_TRUNC(
            'hour',
            b.block_timestamp
        )
        LEFT JOIN royalty_purchases rp
        ON b.tx_hash = rp.tx_hash
        AND b.event_index = rp.event_index
        LEFT JOIN royalty_payments p
        ON b.tx_hash = p.tx_hash
        AND b.royalty_address = p.to_address
        AND rp.agg_id = p.agg_id
)
SELECT
    *
FROM
    FINAL
WHERE
    nft_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
