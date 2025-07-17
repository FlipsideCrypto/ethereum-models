{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver_nft__arbitrage_raw') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
buy AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        'buy' AS trade_side,
        buy_event_index AS event_index,
        buy_platform_name AS platform_name,
        buy_platform_exchange_version AS platform_exchange_version,
        buy_buyer_address AS buyer_address,
        buy_seller_address AS seller_address,
        buy_nft_address AS nft_address,
        buy_tokenid AS tokenid,
        buy_erc1155_value AS erc1155_value,
        buy_project_name AS project_name,
        arb_type,
        funding_source,
        arbitrage_direction,
        CONCAT(
            trade_side,
            '-',
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
        base qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            buy_nft_address,
            buy_tokenid,
            buy_event_index
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
sell AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        'sell' AS trade_side,
        sell_event_index AS event_index,
        sell_platform_name AS platform_name,
        sell_platform_exchange_version AS platform_exchange_version,
        sell_buyer_address AS buyer_address,
        sell_seller_address AS seller_address,
        sell_nft_address AS nft_address,
        sell_tokenid AS tokenid,
        sell_erc1155_value AS erc1155_value,
        sell_project_name AS project_name,
        arb_type,
        funding_source,
        arbitrage_direction,
        CONCAT(
            trade_side,
            '-',
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
        base qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            sell_nft_address,
            sell_tokenid,
            sell_event_index
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
combined AS (
    SELECT
        *
    FROM
        sell
    UNION ALL
    SELECT
        *
    FROM
        buy
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    trade_side,
    event_index,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    tokenid,
    erc1155_value,
    project_name,
    arb_type,
    funding_source,
    arbitrage_direction,
    nft_log_id,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index', 'trade_side', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS nft_arbitrage_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combined
