{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH raw_logs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        contract_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp,
        event_index,
        decoded_flat,
        decoded_flat :direction AS direction,
        decoded_flat :erc20Token :: STRING AS currency_address,
        COALESCE(
            decoded_flat :erc20TokenAmount,
            decoded_flat :erc20FillAmount
        ) :: INT AS total_price_raw,
        COALESCE(
            decoded_flat :erc721Token,
            decoded_flat :erc1155Token
        ) :: STRING AS nft_address,
        COALESCE(
            decoded_flat :erc721TokenId,
            decoded_flat :erc1155TokenId
        ) :: STRING AS tokenId,
        COALESCE(
            decoded_flat :erc1155FillAmount,
            NULL
        ) :: STRING AS erc1155_value,
        decoded_flat :maker :: STRING AS maker,
        decoded_flat :matcher :: STRING AS matcher,
        decoded_flat :taker :: STRING AS taker,
        IFF(
            direction = 0,
            maker,
            taker
        ) AS seller_address,
        IFF(
            direction = 0,
            taker,
            maker
        ) AS buyer_address,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            seller_address,
            buyer_address,
            currency_address
            ORDER BY
                event_index ASC
        ) AS intra_tx_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-03-01'
        AND contract_address = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
        AND event_name IN (
            'ERC721OrderFilled',
            'ERC1155OrderFilled'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
token_transfers_raw AS (
    SELECT
        tx_hash,
        from_address AS buyer_address,
        to_address AS seller_address,
        contract_address AS currency_address,
        raw_amount,
        event_index AS transfers_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            seller_address,
            buyer_address,
            contract_address
            ORDER BY
                event_index ASC
        ) AS intra_tx_index
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-03-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                raw_logs
            WHERE
                currency_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
logs_token_raw AS (
    SELECT
        *,
        IFF(
            raw_amount != total_price_raw,
            'creator_fee_label',
            NULL
        ) AS creator_fee_tag
    FROM
        token_transfers_raw
        JOIN raw_logs USING (
            tx_hash,
            buyer_address,
            currency_address,
            intra_tx_index
        )
),
eth_transfers_raw AS (
    SELECT
        tx_hash,
        to_address AS seller_address,
        eth_value,
        trace_index AS transfers_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            to_address
            ORDER BY
                trace_index ASC
        ) AS intra_tx_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE >= '2022-03-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                raw_logs
            WHERE
                currency_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        )
        AND identifier != 'CALL_ORIGIN'
        AND from_address = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
        AND trace_status = 'SUCCESS'
        AND eth_value > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
logs_eth_raw AS (
    SELECT
        *,
        {# IFF(block_timestamp IS NOT NULL, LEAD(total_price_raw) over (PARTITION BY tx_hash, intra_tx_index
    ORDER BY
        transfers_index ASC
),
0
) AS creator_fee_ #}
IFF(
    block_timestamp IS NOT NULL,
    1,
    0
) AS sale_event_tag,
SUM(sale_event_tag) over (
    PARTITION BY tx_hash
    ORDER BY
        transfers_index ASC
) AS intra_tx_grouping_logs
FROM
    eth_transfers_raw
    LEFT JOIN raw_logs USING (
        tx_hash,
        seller_address,
        intra_tx_index
    )
),
logs_token_fees_agg AS (
    SELECT
        tx_hash,
        intra_tx_index AS intra_tx_grouping,
        SUM(raw_amount) AS creator_fee_raw
    FROM
        logs_token_raw
    WHERE
        creator_fee_tag IS NOT NULL
    GROUP BY
        ALL
),
logs_eth_fees_agg AS (
    SELECT
        tx_hash,
        intra_tx_grouping_logs AS intra_tx_grouping,
        SUM(eth_value) * pow(
            10,
            18
        ) AS creator_fee_raw
    FROM
        logs_eth_raw
    WHERE
        block_timestamp IS NULL
    GROUP BY
        ALL
),
creator_fees_agg AS (
    SELECT
        *
    FROM
        logs_token_fees_agg
    UNION ALL
    SELECT
        *
    FROM
        logs_eth_fees_agg
),
base_sales AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        intra_tx_grouping,
        event_name,
        IFF(
            direction = 0,
            'sale',
            'bid_won'
        ) AS event_type,
        contract_address AS platform_address,
        '0x' AS platform_name,
        '0x v4' AS platform_exchange_version,
        direction,
        matcher,
        maker,
        taker,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        IFF(
            currency_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
            OR currency_address = '0x0000000000000000000000000000000000000000',
            'ETH',
            currency_address
        ) AS currency_address_raw,
        IFF(
            currency_address = '0x0000000000000000000000000000000000000000',
            0,
            total_price_raw
        ) AS total_price_raw,
        COALESCE(
            creator_fee_raw,
            0
        ) AS creator_fee_raw,
        0 AS platform_fee_raw,
        COALESCE(
            creator_fee_raw,
            0
        ) AS total_fees_raw,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
        LEFT JOIN creator_fees_agg USING (
            tx_hash,
            intra_tx_grouping
        )
),
tx_data AS (
    SELECT
        tx_hash,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-03-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    intra_tx_grouping,
    event_name,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    direction,
    matcher,
    maker,
    taker,
    seller_address,
    buyer_address,
    nft_address,
    tokenId,
    erc1155_value,
    currency_address_raw AS currency_address,
    total_price_raw,
    creator_fee_raw,
    platform_fee_raw,
    total_fees_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
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
    _log_id,
    _inserted_timestamp
FROM
    base_sales
    INNER JOIN tx_data USING (tx_hash)
