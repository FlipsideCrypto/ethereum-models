{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH aave_join AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        aave_market AS borrow_asset,
        aave_token AS protocol_market,
        borrowed_tokens,
        borrowed_usd,
        borrower_address,
        aave_version AS platform,
        C.symbol,
        blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_borrows') }} A
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.aave_market = C.address

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
    spark_market AS borrow_asset,
    spark_token AS protocol_market,
    borrowed_tokens,
    borrowed_usd,
    borrower_address,
    platform,
    C.symbol,
    blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__spark_borrows') }} A
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON A.spark_market = C.address

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
    borrow_asset,
    frax_market_address AS protocol_market,
    borrow_amount AS borrowed_tokens,
    borrow_amount_usd borrowed_usd,
    borrower AS borrower_address,
    'Fraxlend' AS platform,
    C.symbol,
    'ethereum' AS blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__fraxlend_borrows') }} A
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON A.frax_market_address = C.address

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
),
borrow_union AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        borrow_asset,
        protocol_market,
        borrowed_tokens,
        borrowed_usd,
        borrower_address,
        platform,
        symbol,
        blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        aave_join
    UNION ALL
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        borrowed_token AS borrow_asset,
        compound_market AS protocol_market,
        borrowed_tokens,
        borrowed_usd AS borrowed_usd,
        borrower_address,
        compound_version AS protocol,
        C.symbol,
        blockchain,
        l._LOG_ID,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__compv3_borrows') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON l.borrowed_token = C.address

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
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    CASE
        WHEN ctoken_symbol = 'cETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE borrows_contract_address
    END AS borrow_asset,
    ctoken AS protocol_market,
    loan_amount AS borrowed_tokens,
    loan_amount_usd AS borrowed_usd,
    borrower AS borrower_address,
    'Compound V2' AS protocol,
    borrows_contract_symbol AS symbol,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__compv2_borrows') }}
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
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    protocol_market,
    borrow_asset,
    borrowed_tokens AS borrow_amount,
    borrowed_usd AS borrow_amount_usd,
    borrower_address,
    platform,
    symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    borrow_union
