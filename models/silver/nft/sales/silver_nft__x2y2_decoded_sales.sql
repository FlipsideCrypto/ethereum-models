{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        decoded_flat :delegateType :: STRING AS delegate_type,
        decoded_flat :intent :: STRING AS intent,
        {# ON intent:
        1 = sell;
maker = nft seller 2 = auction - NOT OUT yet 3 = buy;
maker = nft buyer #}
        decoded_flat :maker :: STRING AS maker,
        decoded_flat :taker :: STRING AS taker,
        CASE
            WHEN intent = 1 THEN maker
            WHEN intent = 3 THEN taker
            ELSE NULL
        END AS seller_address,
        CASE
            WHEN intent = 1 THEN taker
            WHEN intent = 3 THEN maker
            ELSE NULL
        END AS buyer_address,
        CASE
            WHEN intent = 1 THEN 'sale'
            WHEN intent = 3 THEN 'bid_won'
            ELSE NULL
        END AS event_type,
        TRY_BASE64_DECODE_BINARY(
            decoded_flat :item :data :: STRING
        ) :: STRING AS item_decoded,
        LOWER(CONCAT('0x', SUBSTR(item_decoded, 153, 40))) :: STRING AS nft_address,
        (
            utils.udf_hex_to_int(SUBSTR(item_decoded, 193, 64))
        ) :: STRING AS tokenId,
        (
            utils.udf_hex_to_int(SUBSTR(item_decoded, 257, 64))
        ) :: STRING AS tokenId_quantity,
        decoded_flat :detail :price :: INT AS price_raw,
        decoded_flat :detail :fees AS fee_details,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-02-01'
        AND block_number >= 14139341
        AND contract_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
        AND event_name = 'EvInventory'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '100 hours'
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
        VALUE :percentage :: INT / pow(
            10,
            6
        ) AS percent,
        percent * price_raw AS fee_amount_raw,
        VALUE :to :: STRING AS receiver_address
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
        contract_address AS nft_address,
        token_transfer_type
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        contract_address IN (
            SELECT
                nft_address
            FROM
                ev_inventory_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '100 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY contract_address
    ORDER BY
        block_timestamp ASC
) = 1
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
        MAX(_inserted_timestamp) - INTERVAL '100 hours'
    FROM
        {{ this }}
)
{% endif %}
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
        intent,
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
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN NULL
            ELSE tokenId_quantity
        END AS erc1155_value,
        _log_id,
        CONCAT(
            b.nft_address,
            '-',
            b.tokenId,
            '-',
            platform_exchange_version,
            '-',
            _log_id
        ) AS nft_log_id,
        _inserted_timestamp
    FROM
        ev_inventory_base b
        LEFT JOIN fees_agg f USING (
            tx_hash,
            event_index
        )
        LEFT JOIN nft_details n USING (nft_address) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
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
    intent,
    seller_address,
    buyer_address,
    event_type,
    price_raw AS total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    nft_address,
    tokenId,
    erc1155_value,
    from_address AS origin_from_address,
    to_address AS origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    _log_id,
    nft_log_id,
    _inserted_timestamp
FROM
    base_sales b
    INNER JOIN tx_data t
    ON b.tx_hash = t.tx_hash
