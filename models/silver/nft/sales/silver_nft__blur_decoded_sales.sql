{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        decoded_flat,
        decoded_flat: maker :: STRING AS maker,
        decoded_flat: taker :: STRING AS taker,
        decoded_flat: sell :trader :: STRING AS seller_address,
        decoded_flat: buy :trader :: STRING AS buyer_address_temp,
        decoded_flat: sell :side :: INT AS side,
        decoded_flat: sell :matchingPolicy :: STRING AS matching_policy,
        decoded_flat: sell :collection :: STRING AS nft_address,
        decoded_flat: sell :tokenId :: STRING AS tokenId,
        decoded_flat: sell :amount :: INT AS tokenId_quantity,
        decoded_flat: sell :paymentToken :: STRING AS payment_token,
        decoded_flat: sell :price :: INT AS total_price_raw,
        decoded_flat: sell :listingTime :: INT AS listing_time,
        decoded_flat: sell :expirationTime :: INT AS expiration_time,
        decoded_flat: sell :fees AS royalty_array,
        ARRAY_SIZE(
            royalty_array
        ) AS royalty_array_size,
        CONCAT(
            tx_hash,
            '-',
            nft_address,
            '-',
            tokenId
        ) AS tx_nft_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-15'
        AND block_number >= 15779873
        AND contract_address = '0x000000000000ad05ccc4f10045630fb830b95127'
        AND event_name = 'OrdersMatched'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
),
royalty_raw AS (
    SELECT
        tx_hash,
        nft_address,
        tokenId,
        tx_nft_id,
        VALUE,
        VALUE :rate :: INT / pow(
            10,
            4
        ) AS royalty_rate
    FROM
        base,
        LATERAL FLATTEN (
            input => royalty_array
        )
    WHERE
        royalty_array_size > 0
),
royalty_agg AS (
    SELECT
        tx_nft_id,
        SUM(royalty_rate) AS royalty_rate_total
    FROM
        royalty_raw
    GROUP BY
        tx_nft_id
),
buyers_list AS (
    SELECT
        CONCAT(
            tx_hash,
            '-',
            contract_address,
            '-',
            tokenid
        ) AS tx_nft_id,
        erc1155_value,
        to_address
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp >= '2022-10-01'
        AND tx_nft_id IN (
            SELECT
                tx_nft_id
            FROM
                base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_nft_id
    ORDER BY
        event_index DESC
) = 1
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
        block_timestamp :: DATE >= '2022-10-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
),
base_combined AS (
    SELECT
        b.block_number,
        t.block_timestamp,
        b.tx_hash,
        b.event_index,
        CASE
            WHEN payment_token = '0x0000000000a39bb272e79075ade125fd351887ac' THEN 'bid_won'
            WHEN payment_token IN (
                '0x0000000000000000000000000000000000000000',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ) THEN 'sale'
        END AS event_type,
        b.contract_address AS platform_address,
        'blur' AS platform_name,
        'blur v1' AS platform_exchange_version,
        seller_address,
        CASE
            WHEN buyer_address_temp = '0x39da41747a83aee658334415666f3ef92dd0d541' THEN to_address
            ELSE buyer_address_temp
        END AS buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        CASE
            WHEN payment_token = '0x0000000000000000000000000000000000000000' THEN 'ETH'
            ELSE payment_token
        END AS currency_address,
        total_price_raw,
        COALESCE(
            royalty_rate_total,
            0
        ) AS royalty_rate,
        total_price_raw * royalty_rate AS creator_fee_raw,
        0 AS platform_fee_raw,
        creator_fee_raw + platform_fee_raw AS total_fees_raw,
        listing_time,
        expiration_time,
        tx_fee,
        input_data,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        b.tx_nft_id,
        b._log_id,
        b._inserted_timestamp
    FROM
        base b
        INNER JOIN tx_data t
        ON b.tx_hash = t.tx_hash
        INNER JOIN buyers_list l
        ON b.tx_nft_id = l.tx_nft_id
        LEFT OUTER JOIN royalty_agg r
        ON b.tx_nft_id = r.tx_nft_id
    WHERE
        buyer_address IS NOT NULL
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        input_data,
        CONCAT(
            nft_address,
            '-',
            tokenId,
            '-',
            platform_exchange_version,
            '-',
            _log_id
        ) AS nft_log_id,
        tx_nft_id,
        _inserted_timestamp
    FROM
        base_combined qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    *
FROM
    FINAL
