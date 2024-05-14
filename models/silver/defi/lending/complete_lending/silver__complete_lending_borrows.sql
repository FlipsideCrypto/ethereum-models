-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated','heal']
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
        borrowed_tokens_unadj AS amount_unadj,
        borrowed_tokens AS amount,
        borrowed_usd AS amount_usd,
        aave_version AS platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_borrows') }} A

{% if is_incremental() and 'aave' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__spark_borrows') }} A

{% if is_incremental() and 'spark' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__radiant_borrows') }} A

{% if is_incremental() and 'radiant' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__sturdy_borrows') }} A

{% if is_incremental() and 'sturdy' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__uwu_borrows') }} A

{% if is_incremental() and 'uwu' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__flux_borrows') }} A

{% if is_incremental() and 'flux' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__cream_borrows') }} A

{% if is_incremental() and 'cream' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__strike_borrows') }} A

{% if is_incremental() and 'strike' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
        FROM
            {{ this }}
    )
{% endif %}
),
silo AS (
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
        NULL AS amount_usd,
        platform,
        'ethereum' AS blockchain,
        l._LOG_ID,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__silo_borrows') }}
        l

{% if is_incremental() and 'silo' not in var('HEAL_MODELS') %}
WHERE
    l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
        FROM
            {{ this }}
    )
{% endif %}
),
fraxlend AS (
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
        frax_market_address AS protocol_market,
        borrow_asset AS token_address,
        borrow_symbol AS symbol,
        borrow_amount_unadj AS amount_unadj,
        borrow_amount AS amount,
        NULL AS amount_usd,
        'Fraxlend' AS platform,
        'ethereum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__fraxlend_borrows') }} A

{% if is_incremental() and 'fraxlend' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
        FROM
            {{ this }}
    )
{% endif %}
),
comp AS (
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
        ctoken AS protocol_market,
        CASE
            WHEN ctoken_symbol = 'cETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE borrows_contract_address
        END AS token_address,
        borrows_contract_symbol AS token_symbol,
        loan_amount_raw AS amount_unadj,
        loan_amount AS amount,
        loan_amount_usd AS amount_usd,
        compound_version AS platform,
        'ethereum' AS blockchain,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__comp_borrows') }}

{% if is_incremental() and 'comp' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
        FROM
            {{ this }}
    )
{% endif %}
),
borrow_union AS (
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
complete_lending_borrows AS (
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
        CASE
            WHEN platform = 'Fraxlend' THEN 'BorrowAsset'
            WHEN platform = 'Compound V3' THEN 'Withdraw'
            ELSE 'Borrow'
        END AS event_name,
        protocol_market,
        b.token_address,
        b.token_symbol,
        amount_unadj,
        amount,
        CASE
            WHEN platform NOT IN (
                'Aave',
                'Compound'
            ) THEN ROUND((amount * price), 2)
            ELSE amount_usd
        END AS amount_usd,
        platform,
        b.blockchain,
        b._LOG_ID,
        b._INSERTED_TIMESTAMP
    FROM
        borrow_union b
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON b.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
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
        event_name,
        protocol_market,
        t0.token_address,
        t0.token_symbol,
        amount_unadj,
        amount,
        CASE
            WHEN platform NOT IN (
                'Aave',
                'Compound'
            ) THEN ROUND((amount * price), 2)
            ELSE amount_usd
        END AS amount_usd,
        platform,
        t0.blockchain,
        t0._LOG_ID,
        t0._INSERTED_TIMESTAMP
    FROM
        {{ this }}
        t0
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON t0.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
    WHERE
        CONCAT(
            t0.block_number,
            '-',
            t0.platform
        ) IN (
            SELECT
                CONCAT(
                    t1.block_number,
                    '-',
                    t1.platform
                )
            FROM
                {{ this }}
                t1
            WHERE
                t1.amount_usd IS NULL
                AND t1._inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('silver__complete_token_prices') }}
                        p
                    WHERE
                        p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND p.price IS NOT NULL
                        AND p.token_address = t1.token_address
                        AND p.hour = DATE_TRUNC(
                            'hour',
                            t1.block_timestamp
                        )
                )
            GROUP BY
                1
        )
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        complete_lending_borrows

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    *
FROM
    heal_model
{% endif %}
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
