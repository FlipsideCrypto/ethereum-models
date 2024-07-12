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
    sell_event_index,
    sell_platform_name,
    sell_platform_exchange_version,
    sell_buyer_address,
    sell_seller_address,
    sell_nft_address,
    sell_tokenid,
    sell_erc1155_value,
    sell_project_name,
    _inserted_timestamp -- add PK
FROM
    base qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        sell_nft_address,
        sell_tokenid,
        sell_event_index
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
