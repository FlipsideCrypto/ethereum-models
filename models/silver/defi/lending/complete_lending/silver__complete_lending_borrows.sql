{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH borrow_union AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        aave_market AS borrow_asset,
        aave_token AS protocol_market,
        borrowed_tokens,
        borrowed_usd,
        borrower_address,
        aave_version AS platform,
        symbol,
        blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_borrows') }} A

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    spark_market AS borrow_asset,
    spark_token AS protocol_market,
    borrowed_tokens,
    NULL AS borrowed_usd,
    borrower_address,
    platform,
    symbol,
    blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__spark_borrows') }} A

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrow_asset,
    frax_market_address AS protocol_market,
    borrow_amount AS borrowed_tokens,
    NULL AS borrowed_usd,
    borrower AS borrower_address,
    'Fraxlend' AS platform,
    borrow_symbol AS symbol,
    'ethereum' AS blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__fraxlend_borrows') }} A

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    CASE
        WHEN ctoken_symbol = 'cETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE borrows_contract_address
    END AS borrow_asset,
    ctoken AS protocol_market,
    loan_amount AS borrowed_tokens,
    loan_amount_usd AS borrowed_usd,
    borrower AS borrower_address,
    compound_version AS platform,
    borrows_contract_symbol AS symbol,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__comp_borrows') }}
    l

{% if is_incremental() %}
WHERE
    l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        CASE
            WHEN platform = 'Fraxlend' THEN 'BorrowAsset'
            WHEN platform = 'Compound V3' THEN 'Withdraw'
            ELSE 'Borrow'
        END AS event_name,
        protocol_market,
        borrow_asset,
        borrowed_tokens AS borrow_amount,
        CASE
            WHEN platform IN (
                'Fraxlend',
                'Spark'
            ) THEN ROUND((borrowed_tokens * price), 2)
            ELSE ROUND(
                borrowed_usd,
                2
            )
        END AS borrow_amount_usd,
        borrower_address,
        platform,
        b.symbol,
        blockchain,
        b._LOG_ID,
        b._INSERTED_TIMESTAMP
    FROM
        borrow_union b
        LEFT JOIN {{ ref('core__fact_hourly_token_prices') }}
        p
        ON borrow_asset = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON b.borrow_asset = C.address
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
