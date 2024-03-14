{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH aave AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol AS token_symbol,
        borrowed_tokens_unadj as amount_unadj,
        borrowed_tokens as amount,
        aave_version as platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_borrows') }} A

{% if is_incremental() and 'aave' not in var('HEAL_CURATED_MODEL') %}
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
),
spark AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        spark_token AS protocol_market,
        spark_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__spark_borrows') }} A

{% if is_incremental() and 'spark' not in var('HEAL_CURATED_MODEL') %}
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
),
radiant AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        radiant_token AS protocol_market,
        radiant_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__radiant_borrows') }} A

{% if is_incremental() and 'radiant' not in var('HEAL_CURATED_MODEL') %}
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
),
sturdy AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        sturdy_token AS protocol_market,
        sturdy_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__sturdy_borrows') }} A

{% if is_incremental() and 'sturdy' not in var('HEAL_CURATED_MODEL') %}
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
),
uwu AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        uwu_token AS protocol_market,
        uwu_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__uwu_borrows') }} A

{% if is_incremental() and 'uwu' not in var('HEAL_CURATED_MODEL') %}
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
),
flux AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower,
        token_address AS protocol_market,
        borrows_contract_address AS token_address,
        borrows_contract_symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'optimism' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__flux_borrows') }} A

{% if is_incremental() and 'flux' not in var('HEAL_CURATED_MODEL') %}
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
),
cream AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower,
        token_address AS protocol_market,
        borrows_contract_address AS token_address,
        borrows_contract_symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'optimism' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__cream_borrows') }} A

{% if is_incremental() and 'cream' not in var('HEAL_CURATED_MODEL') %}
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
),
strike AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower,
        token_address AS protocol_market,
        borrows_contract_address AS token_address,
        borrows_contract_symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'optimism' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__strike_borrows') }} A

{% if is_incremental() and 'strike' not in var('HEAL_CURATED_MODEL') %}
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
),
silo as (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower,
        silo_market AS protocol_market,
        token_address,
        token_symbol,
        amount_unadj,
        amount,
        platform,
        'ethereum' AS blockchain,
        l._LOG_ID,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__silo_borrows') }}
        l

    {% if is_incremental() and 'silo' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        l._inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),
fraxlend as (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrow_asset as token_address,
        frax_market_address AS protocol_market,
        borrow_amount_unadj AS amount_unadj,
        borrow_amount AS amount,
        borrower,
        'Fraxlend' AS platform,
        borrow_symbol AS symbol,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__fraxlend_borrows') }} A

    {% if is_incremental() and 'fraxlend' not in var('HEAL_CURATED_MODEL') %}
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
),
comp as (
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
        END AS token_address,
        ctoken AS protocol_market,
        loan_amount_raw AS amount,
        loan_amount AS amount_unadj,
        borrower,
        compound_version AS platform,
        borrows_contract_symbol AS token_symbol,
        'ethereum' AS blockchain,
        l._LOG_ID,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__comp_borrows') }}
        l

    {% if is_incremental() and 'comp' not in var('HEAL_CURATED_MODEL') %}
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
borrow_union as (
    SELECT
        *
    FROM
        aave
    UNION ALL
    SELECT
        *
    FROM
        comp
    UNION ALL
    SELECT
        *
    FROM
        cream
    UNION ALL
    SELECT
        *
    FROM
        flux
    UNION ALL
    SELECT
        *
    FROM
        fraxlend
    UNION ALL
    SELECT
        *
    FROM
        radiant
    UNION ALL
    SELECT
        *
    FROM
        silo
    UNION ALL
    SELECT
        *
    FROM
        spark
    UNION ALL
    SELECT
        *
    FROM
        strike
    UNION ALL
    SELECT
        *
    FROM
        sturdy
    UNION ALL
    SELECT
        *
    FROM
        uwu
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
        b.token_address,
        amount_unadj,
        amount,
        ROUND((amount * price), 2) as amount_usd,
        borrower,
        platform,
        b.token_symbol,
        blockchain,
        b._LOG_ID,
        b._INSERTED_TIMESTAMP
    FROM
        borrow_union b
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON b.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON b.token_address = C.address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_borrows_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
