{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH punk_sales AS (

    SELECT
        _log_id,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_index,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS seller_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS buyer_address,
        PUBLIC.udf_hex_to_int(
            DATA :: STRING
        ) :: INTEGER AS sale_value,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INTEGER AS token_id,
        _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = LOWER('0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB')
        AND topics [0] :: STRING = '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3'
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
nft_transfers AS (
    SELECT
        nft.tx_hash AS tx_hash,
        'sale' AS event_type,
        nft.from_address AS seller_address,
        nft.to_address AS buyer_address,
        nft.contract_address AS nft_address,
        nft.tokenid,
        'ETH' AS currency_symbol,
        'ETH' AS currency_address,
        nft.erc1155_value,
        'larva labs' AS platform_name,
        'cryptopunks' AS platform_exchange_version,
        contract_address AS platform_address,
        nft._log_id AS _log_id,
        nft._inserted_timestamp AS _inserted_timestamp,
        nft.event_index AS event_index,
        ROW_NUMBER() over(
            PARTITION BY nft.tx_hash
            ORDER BY
                nft.event_index ASC
        ) AS agg_id
    FROM
        {{ ref('silver__nft_transfers') }} AS nft
    WHERE
        nft.tx_hash IN (
            SELECT
                tx_hash
            FROM
                punk_sales
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
nft_transactions AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data :: STRING AS input,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        PUBLIC.udf_hex_to_int(
            segmented_input [1] :: STRING
        ) / pow(
            10,
            18
        ) AS sale_amt,
        CASE
            WHEN origin_function_signature = '0x23165b75' THEN sale_amt
            ELSE VALUE
        END AS tx_price,
        0 AS total_fees_usd,
        0 AS platform_fee_usd,
        0 AS creator_fee_usd,
        0 AS total_fees,
        0 AS platform_fee,
        0 AS creator_fee,
        _inserted_timestamp,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                punk_sales
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
eth_prices AS (
    SELECT
        HOUR,
        (price) AS eth_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
FINAL AS (
    SELECT
        punk_sales.block_number,
        punk_sales.block_timestamp,
        punk_sales.tx_hash,
        origin_to_address,
        origin_from_address,
        origin_function_signature,
        CASE
            WHEN origin_function_signature = '0x23165b75' THEN 'bid_won'
            ELSE 'sale'
        END AS event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        nft_transfers.buyer_address,
        nft_transfers.seller_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_symbol,
        currency_address,
        CASE
            WHEN origin_function_signature = '0x23165b75' THEN tx_price
            ELSE (sale_value / pow(10, 18))
        END AS price,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        ROUND(
            eth_price * price,
            2
        ) AS price_usd,
        total_fees,
        platform_fee,
        creator_fee,
        total_fees_usd,
        platform_fee_usd,
        creator_fee_usd,
        tx_fee,
        punk_sales._log_id,
        CONCAT(
            nft_address,
            '-',
            tokenId,
            '-',
            platform_exchange_version,
            '-',
            punk_sales._log_id
        ) AS nft_log_id,
        punk_sales._inserted_timestamp,
        input_data
    FROM
        punk_sales
        LEFT JOIN nft_transfers
        ON nft_transfers.tx_hash = punk_sales.tx_hash
        LEFT JOIN nft_transactions
        ON nft_transactions.tx_hash = punk_sales.tx_hash
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'hour',
            punk_sales.block_timestamp
        )
)
SELECT
    *
FROM
    FINAL
WHERE
    nft_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
