{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_models AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        platform_name,
        platform_address,
        platform_exchange_version,
        contract_address,
        decoded_flat,
        loanId,
        lender_address,
        borrower_address,
        nft_address,
        tokenId,
        principal_amount_unadj,
        loan_token_address,
        interest_rate,
        interest_rate_bps,
        interest_rate_percentage,
        'perpetual' AS loan_term_type,
        loan_start_timestamp,
        NULL AS loan_due_timestamp,
        _log_id,
        _inserted_timestamp,
        nft_lending_id,
        unique_loan_id
    FROM
        {{ ref('silver_nft__blend_liquidations') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    contract_address,
    decoded_flat,
    loanId,
    lender_address,
    borrower_address,
    nft_address,
    tokenId,
    principal_amount_unadj,
    loan_token_address,
    interest_rate,
    interest_rate_bps,
    interest_rate_percentage,
    'fixed' AS loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__nftfi_v1_liquidations') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    contract_address,
    decoded_flat,
    loanId,
    lender_address,
    borrower_address,
    nft_address,
    tokenId,
    principal_amount_unadj,
    loan_token_address,
    interest_rate,
    interest_rate_bps,
    interest_rate_percentage,
    'fixed' AS loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__nftfi_v2_liquidations') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
prices_raw AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT loan_token_address
            FROM
                base_models
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base_models
        )
),
all_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        '0x0000000000a39bb272e79075ade125fd351887ac' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
eth_price AS (
    SELECT
        HOUR,
        hourly_prices AS eth_price_hourly
    FROM
        prices_raw
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2020-05-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_models
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    contract_address,
    decoded_flat,
    loanId,
    lender_address,
    borrower_address,
    nft_address,
    tokenId,
    principal_amount_unadj,
    CASE
        WHEN loan_token_address IN (
            'ETH',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            '0x0000000000a39bb272e79075ade125fd351887ac'
        ) THEN principal_amount_unadj / pow(
            10,
            18
        )
        ELSE COALESCE (principal_amount_unadj / pow(10, p.decimals), NULL)
    END AS principal_amount,
    IFF(
        p.decimals IS NULL,
        NULL,
        principal_amount * hourly_prices
    ) AS principal_amount_usd,
    loan_token_address,
    interest_rate,
    interest_rate_bps,
    interest_rate_percentage,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    tx_fee * eth_price_hourly AS tx_fee_usd,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_models b
    LEFT JOIN all_prices p
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = p.hour
    AND b.loan_token_address = p.token_address
    LEFT JOIN eth_price e
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = e.hour
    LEFT JOIN tx_data USING (tx_hash)
