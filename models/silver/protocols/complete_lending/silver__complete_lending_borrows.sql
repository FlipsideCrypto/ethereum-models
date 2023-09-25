{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

WITH aave_join AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        aave_market as borrow_asset,
        aave_token as protocol_token,
        borrowed_tokens,
        borrowed_usd,
        borrower_address,
        borrow_rate_mode,
        lending_pool_contract,
        aave_version as platform,
        C.symbol,
        blockchain,
        A._LOG_ID,
        C._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_ez_borrows') }} A
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.aave_market = C.address
{% if is_incremental() %}
WHERE
    a._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
        spark_market as borrow_asset,
        spark_token as protocol_token,
        borrowed_tokens,
        borrowed_usd,
        borrower_address,
        borrow_rate_mode,
        lending_pool_contract,
        platform,
        C.symbol,
        blockchain,
        A._LOG_ID,
        C._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__spark_ez_borrows') }} A
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.spark_market = C.address
{% if is_incremental() %}
WHERE
    a._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
        frax_market_address AS protocol_token,
        borrow_amount AS borrowed_tokens,
        borrow_amount_usd borrowed_usd,
        borrower AS borrower_address,
        NULL AS borrow_rate_mode,
        NULL AS lending_pool_contract,
        'Fraxlend' AS platform,
        C.symbol,
        'ethereum' AS blockchain,
        A._LOG_ID,
        C._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__fraxlend_ez_borrows') }} A
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.frax_market_address = C.address
{% if is_incremental() %}
WHERE
    a._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
        protocol_token,
        borrowed_tokens,
        borrowed_usd,
        borrower_address,
        borrow_rate_mode,
        lending_pool_contract,
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
        protocol_token,
        borrowed_tokens,
        tokens_usd AS borrowed_usd,
        borrower_address,
        NULL AS borrow_rate_mode,
        NULL AS lending_pool_contract,
        compound_version AS protocol,
        symbol,
        blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__compv3_ez_borrows') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
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
    borrows_contract_address AS borrow_asset,
    ctoken AS protocol_token,
    loan_amount AS borrowed_tokens,
    loan_amount_usd AS borrowed_usd,
    borrower AS borrower_address,
    NULL AS borrow_rate_mode,
    NULL AS lending_pool_contract,
    'Compound V2' AS protocol,
    ctoken_symbol AS symbol,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('compound__ez_borrows') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *
FROM
    borrow_union
