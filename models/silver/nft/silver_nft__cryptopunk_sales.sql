{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
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
        utils.udf_hex_to_int(
            DATA :: STRING
        ) :: INTEGER AS sale_value,
        utils.udf_hex_to_int(
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
        ) :: DATE
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
        ) :: DATE - 1
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
        utils.udf_hex_to_int(
            segmented_input [1] :: STRING
        ) AS sale_amt,
        CASE
            WHEN origin_function_signature = '0x23165b75' THEN sale_amt
            ELSE VALUE * pow(
                10,
                18
            )
        END AS tx_price,
        0 AS total_fees_raw,
        0 AS platform_fee_raw,
        0 AS creator_fee_raw,
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
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
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
        currency_address,
        (
            CASE
                WHEN origin_function_signature = '0x23165b75' THEN tx_price
                ELSE sale_value
            END
        ) AS total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
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
)
SELECT
    *
FROM
    FINAL
WHERE
    nft_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
