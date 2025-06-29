{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number', 'platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE', 'platform_name'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, event_name, event_type, platform_address, platform_exchange_version, lender_address, borrower_address, nft_address, project_name, loan_token_address, loan_token_symbol, loan_term_type), SUBSTRING(origin_function_signature, event_name, event_type, platform_address, platform_exchange_version, lender_address, borrower_address, nft_address, project_name, loan_token_address, loan_token_symbol, loan_term_type)",
    tags = ['silver','nft','curated']
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
        borrower_address,
        lender_address,
        loanid,
        nft_address,
        tokenId,
        principal_unadj,
        debt_unadj,
        loan_token_address,
        interest_rate_percentage,
        annual_percentage_rate,
        platform_fee_unadj,
        event_type,
        loan_term_type,
        loan_start_timestamp,
        loan_due_timestamp,
        loan_tenure,
        _log_id,
        _inserted_timestamp,
        nft_lending_id,
        unique_loan_id
    FROM
        {{ ref('silver_nft__blend_loans') }}

{% if is_incremental() and 'blend' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    borrower_address,
    lender_address,
    loanid,
    nft_address,
    tokenId,
    principal_unadj,
    debt_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    platform_fee_unadj,
    event_type,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__nftfi_v2_loans') }}

{% if is_incremental() and 'nftfi_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    borrower_address,
    lender_address,
    loanid,
    nft_address,
    tokenId,
    principal_unadj,
    debt_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    platform_fee_unadj,
    event_type,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__nftfi_v1_loans') }}

{% if is_incremental() and 'nftfi_v1' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    borrower_address,
    lender_address,
    loanid,
    nft_address,
    tokenId,
    principal_unadj,
    debt_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    platform_fee_unadj,
    event_type,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__arcade_v1_loans') }}

{% if is_incremental() and 'arcade_v1' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    borrower_address,
    lender_address,
    loanid,
    nft_address,
    tokenId,
    principal_unadj,
    debt_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    platform_fee_unadj,
    event_type,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__arcade_v2_loans') }}

{% if is_incremental() and 'arcade_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    borrower_address,
    lender_address,
    loanid,
    nft_address,
    tokenId,
    principal_unadj,
    debt_unadj,
    loan_token_address,
    interest_rate_percentage,
    annual_percentage_rate,
    platform_fee_unadj,
    event_type,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id
FROM
    {{ ref('silver_nft__arcade_v3_loans') }}

{% if is_incremental() and 'arcade_v3' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        {{ ref('price__ez_prices_hourly') }}
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
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2020-05-01'

{% if is_incremental() and 'transactions' not in var('HEAL_MODELS') %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        event_name,
        platform_name,
        platform_address,
        platform_exchange_version,
        loanid,
        lender_address,
        borrower_address,
        nft_address,
        tokenId,
        C.name AS project_name,
        principal_unadj,
        CASE
            WHEN loan_token_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN principal_unadj / pow(
                10,
                18
            )
            ELSE COALESCE (principal_unadj / pow(10, p.decimals), NULL)
        END AS principal,
        IFF(
            p.decimals IS NULL,
            NULL,
            principal * hourly_prices
        ) AS principal_usd,
        debt_unadj :: INT AS debt_unadj,
        CASE
            WHEN loan_token_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN debt_unadj :: INT / pow(
                10,
                18
            )
            ELSE COALESCE (debt_unadj :: INT / pow(10, p.decimals), NULL)
        END AS debt,
        IFF(
            p.decimals IS NULL,
            NULL,
            debt * hourly_prices
        ) AS debt_usd,
        platform_fee_unadj,
        CASE
            WHEN loan_token_address IN (
                'ETH',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0x0000000000a39bb272e79075ade125fd351887ac'
            ) THEN platform_fee_unadj / pow(
                10,
                18
            )
            ELSE COALESCE (platform_fee_unadj / pow(10, p.decimals), NULL)
        END AS platform_fee,
        IFF(
            p.decimals IS NULL,
            NULL,
            platform_fee * hourly_prices
        ) AS platform_fee_usd,
        loan_token_address,
        p.symbol AS loan_token_symbol,
        interest_rate_percentage AS interest_rate,
        annual_percentage_rate AS apr,
        loan_term_type,
        loan_start_timestamp,
        loan_due_timestamp,
        loan_tenure,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_fee,
        tx_fee * eth_price_hourly AS tx_fee_usd,
        _log_id,
        b._inserted_timestamp,
        nft_lending_id,
        unique_loan_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        base_models b
        INNER JOIN tx_data USING (tx_hash)
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
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON b.nft_address = C.address

{% if is_incremental() and 'heal_tx' in var('HEAL_MODELS') %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    event_name,
    platform_name,
    platform_address,
    platform_exchange_version,
    loanid,
    lender_address,
    borrower_address,
    nft_address,
    tokenId,
    project_name,
    principal_unadj,
    principal,
    principal_usd,
    debt_unadj,
    debt,
    debt_usd,
    platform_fee_unadj,
    platform_fee,
    platform_fee_usd,
    loan_token_address,
    loan_token_symbol,
    interest_rate,
    apr,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f.tx_fee,
    f.tx_fee * eth_price_hourly AS tx_fee_usd,
    _log_id,
    _inserted_timestamp,
    nft_lending_id,
    unique_loan_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ this }}
    t
    INNER JOIN tx_data f USING (tx_hash)
    LEFT JOIN eth_price e
    ON DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = e.hour
WHERE
    t.tx_fee IS NULL
{% endif %}
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY nft_lending_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
