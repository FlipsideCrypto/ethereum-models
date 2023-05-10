{{ config(
    materialized = 'incremental',
    unique_key = "nft_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH nft_base_models AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        currency_symbol,
        currency_address,
        price,
        price_usd,
        total_fees,
        platform_fee,
        creator_fee,
        total_fees_usd,
        platform_fee_usd,
        creator_fee_usd,
        tx_fee,
        tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        nft_log_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_nft__looksrare_decoded_sales') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
) {# metadata AS (
SELECT
    project_address,
    project_name,
    token_id,
    token_metadata
FROM
    {{ ref('silver__nft_labels_temp') }}
WHERE
    project_address IS NOT NULL
),
labels_only AS (
    SELECT
        DISTINCT project_address AS project_address,
        project_name
    FROM
        metadata
) #}
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    CASE
        WHEN origin_to_address IN (
            '0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5',
            '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2'
        ) THEN 'gem'
        WHEN origin_to_address IN (
            '0x39da41747a83aee658334415666f3ef92dd0d541',
            '0x000000000000ad05ccc4f10045630fb830b95127'
        ) THEN 'blur'
        ELSE platform_name
    END AS platform_name,
    platform_exchange_version,
    CASE
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b'
        AND block_timestamp :: DATE <= '2023-04-04' THEN 'Gem'
        WHEN RIGHT(
            input_data,
            8
        ) = '72db8c0b'
        AND block_timestamp :: DATE >= '2023-04-05' THEN 'OpenSea Pro'
        WHEN RIGHT(
            input_data,
            8
        ) = '332d1229' THEN 'Blur'
        ELSE NULL
    END AS aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    -- l.project_name,
    erc1155_value,
    tokenId,
    -- m.token_metadata,
    currency_symbol,
    currency_address,
    price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    nft_log_id,
    input_data,
    _log_id,
    _inserted_timestamp
FROM
    nft_base_models b {# LEFT JOIN labels_only l
    ON b.nft_address = l.project_address
    LEFT JOIN metadata m
    ON b.nft_address = m.project_address
    AND b.tokenId = m.token_id #}
