{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}
-- step 1 - find events where a trade occured on the relevant exchanges, in this case, x2y2, wyvern v1, wyvern v2, and seaport
WITH relevant_txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'
        AND (
            (
                --opensea
                contract_address IN (
                    '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b',
                    '0x7f268357a8c2552623316e2562d90e642bb538e5'
                )
                AND event_name = 'OrdersMatched'
            )
            OR (
                -- seaport
                contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'
                AND topics [0] :: STRING = '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
            )
            OR (
                -- x2y2
                contract_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
                AND topics [0] :: STRING = '0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33'
            )
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
-- step 2 - pull all the traces data for our txs, we will need to use this a few places
traces_data AS (
    SELECT
        *
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                relevant_txs
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
-- step 3 - find all token transfer events in these txs using execution traces, including tranfer single function for erc1155
traces_token_tranfers AS (
    SELECT
        tx_hash,
        CONCAT('0x', SUBSTR(input, 35, 40)) AS address1,
        -- nft_from_address
        CONCAT('0x', SUBSTR(input, 99, 40)) AS address2,
        --nft_to_address
        udf_hex_to_int(SUBSTR(input, 139, 64)) AS tokenid,
        to_address AS nft_address,
        input,
        CASE
            WHEN from_address = '0x1e0049783f008a0085193e00003d00cd54003c71' THEN 'seaport'
            WHEN from_address = '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2' THEN 'x2y2'
            WHEN wyvern_proxy_address IS NOT NULL THEN 'opensea'
        END AS call_source
    FROM
        traces_data
        LEFT JOIN {{ ref('silver_nft__opensea_proxy') }}
        ON from_address = wyvern_proxy_address
        AND creator = 'opensea'
    WHERE
        LEFT(
            input,
            10
        ) IN (
            '0x23b872dd',
            '0xf242432a'
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_hash, address1, tokenid
    ORDER BY
        call_source DESC nulls last)) = 1
),
-- step 4 - find the emitted NFT transfer events in these txs
nft_transfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                relevant_txs
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
)
