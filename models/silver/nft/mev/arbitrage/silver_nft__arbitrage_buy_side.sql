{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    buy_event_index,
    buy_platform_name,
    buy_platform_exchange_version,
    buy_buyer_address,
    buy_seller_address,
    buy_nft_address,
    buy_tokenid,
    buy_erc1155_value,
    buy_project_name,
    _inserted_timestamp -- add PK
FROM
    base qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        buy_nft_address,
        buy_tokenid,
        buy_event_index
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
