{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH seller_base AS (

    SELECT
        b.block_number,
        b.block_timestamp,
        tx_hash,
        intra_tx_grouping,
        tx_position,
        b.event_index,
        b.from_address,
        nft_to_address_logs,
        COALESCE(
            b.from_address,
            nft_to_address_logs
        ) AS buyer_address,
        COALESCE(
            b.to_address,
            nft_address
        ) AS platform_address,
        function_sig,
        purchase_contract,
        mint_eth_value * pow(
            10,
            18
        ) AS mint_eth_raw,
        revenue_split_price_raw,
        revenue_split_platform_raw,
        nft_address,
        project_id,
        tokenid,
        artist_address,
        transfers_hash,
        transfers_from_address,
        transfers_to_address,
        transfers_event_index,
        transfers_contract_address,
        token_contract_address,
        transfers_raw_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__artblocks_base_sales') }}
        b
        INNER JOIN {{ ref('silver_nft__artblocks_seller_reads') }}
        s USING (
            nft_address,
            project_id
        )

{% if is_incremental() %}
WHERE
    b._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
token_transfers_count_filter AS (
    SELECT
        tx_hash,
        COUNT(1) AS transfers_count
    FROM
        seller_base
    WHERE
        mint_eth_raw = 0
        AND transfers_hash IS NOT NULL
        AND tx_position IS NOT NULL
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                seller_base
            WHERE
                artist_address = transfers_to_address
                OR token_contract_address = transfers_contract_address
        )
    GROUP BY
        tx_hash
),
token_sales_raw AS (
    SELECT
        *,
        IFF(
            transfers_count > 1,
            'true',
            'false'
        ) AS fees_type
    FROM
        seller_base
        INNER JOIN token_transfers_count_filter USING(tx_hash)
    WHERE
        mint_eth_raw = 0
        AND transfers_hash IS NOT NULL
        AND tx_position IS NOT NULL
),
token_sales_agg AS (
    SELECT
        tx_hash,
        fees_type,
        transfers_from_address,
        transfers_contract_address,
        SUM(transfers_raw_amount) AS total_token_transferred
    FROM
        token_sales_raw
    GROUP BY
        ALL
),
token_sales_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        event_index,
        platform_address,
        artist_address,
        artist_address AS seller_address,
        buyer_address,
        function_sig,
        purchase_contract,
        nft_address,
        project_id,
        tokenid,
        A.transfers_contract_address AS currency_address,
        A.total_token_transferred AS total_price_raw,
        IFF(
            fees_type = 'true',
            A.total_token_transferred / pow(
                10,
                1
            ),
            0
        ) AS platform_fee_raw,
        0 AS creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        _log_id,
        _inserted_timestamp
    FROM
        seller_base b
        INNER JOIN token_sales_agg A USING (tx_hash)
    WHERE
        mint_eth_raw = 0
        AND transfers_hash IS NOT NULL
        AND tx_position IS NOT NULL qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            intra_tx_grouping
            ORDER BY
                transfers_event_index ASC
        ) = 1
),
eth_sales_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        intra_tx_grouping,
        event_index,
        platform_address,
        artist_address,
        artist_address AS seller_address,
        buyer_address,
        function_sig,
        purchase_contract,
        nft_address,
        project_id,
        tokenid,
        'ETH' AS currency_address,
        CASE
            WHEN nft_address IN (
                '0x145789247973c5d612bf121e9e4eef84b63eb707',
                '0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36',
                '0x64780ce53f6e966e18a22af13a2f97369580ec11'
            ) THEN COALESCE(
                revenue_split_price_raw,
                mint_eth_raw
            )
            ELSE mint_eth_raw
        END AS total_price_raw,
        CASE
            WHEN nft_address IN (
                '0x145789247973c5d612bf121e9e4eef84b63eb707',
                '0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36',
                '0x64780ce53f6e966e18a22af13a2f97369580ec11'
            ) THEN COALESCE(
                revenue_split_platform_raw,
                mint_eth_raw / pow(
                    10,
                    1
                )
            )
            ELSE mint_eth_raw / pow(
                10,
                1
            )
        END AS platform_fee_raw,
        0 AS creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        _log_id,
        _inserted_timestamp
    FROM
        seller_base
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                token_sales_base
        )
),
final_base AS (
    SELECT
        *
    FROM
        token_sales_base
    UNION ALL
    SELECT
        *
    FROM
        eth_sales_base
),
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2020-11-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    intra_tx_grouping,
    event_index,
    'mint' AS event_type,
    platform_address,
    'art blocks' AS platform_name,
    'art blocks' AS platform_exchange_version,
    artist_address,
    seller_address,
    buyer_address,
    function_sig,
    purchase_contract,
    nft_address,
    project_id,
    tokenid,
    NULL AS erc1155_value,
    currency_address,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    total_fees_raw,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    _log_id,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    _inserted_timestamp
FROM
    final_base
    INNER JOIN tx_data USING (tx_hash) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
