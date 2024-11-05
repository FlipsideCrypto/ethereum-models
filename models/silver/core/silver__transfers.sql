-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, contract_address, from_address, to_address, symbol), SUBSTRING(origin_function_signature, contract_address, from_address, to_address, symbol)",
    tags = ['realtime','reorg','heal']
) }}

WITH logs AS (

    SELECT
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address :: STRING AS contract_address,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topics [2], 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        raw_amount_precise :: FLOAT AS raw_amount,
        event_index,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
{% endif %}
),
token_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        t.contract_address,
        from_address,
        to_address,
        raw_amount_precise,
        raw_amount,
        IFF(
            C.decimals IS NOT NULL,
            utils.udf_decimal_adjust(
                raw_amount_precise,
                C.decimals
            ),
            NULL
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        IFF(
            C.decimals IS NOT NULL
            AND price IS NOT NULL,
            amount * price,
            NULL
        ) AS amount_usd,
        C.decimals AS decimals,
        C.symbol AS symbol,
        price AS token_price,
        CASE
            WHEN C.decimals IS NULL THEN 'false'
            ELSE 'true'
        END AS has_decimal,
        CASE
            WHEN price IS NULL THEN 'false'
            ELSE 'true'
        END AS has_price,
        t._log_id,
        t._inserted_timestamp
    FROM
        logs t
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON t.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = HOUR
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON t.contract_address = C.address
    WHERE
        raw_amount IS NOT NULL
        AND to_address IS NOT NULL
        AND from_address IS NOT NULL
)

{% if is_incremental() and var(
    'HEAL_MODEL'
) %},
heal_model AS (
    SELECT
        t0.block_number,
        t0.block_timestamp,
        t0.tx_hash,
        t0.event_index,
        t0.origin_function_signature,
        t0.origin_from_address,
        t0.origin_to_address,
        t0.contract_address,
        t0.from_address,
        t0.to_address,
        t0.raw_amount_precise,
        t0.raw_amount,
        IFF(
            C.decimals IS NOT NULL,
            utils.udf_decimal_adjust(
                t0.raw_amount_precise,
                C.decimals
            ),
            NULL
        ) AS amount_precise_heal,
        amount_precise_heal :: FLOAT AS amount_heal,
        IFF(
            C.decimals IS NOT NULL
            AND price IS NOT NULL,
            amount_heal * p.price,
            NULL
        ) AS amount_usd,
        C.decimals AS decimals,
        C.symbol AS symbol,
        p.price AS token_price,
        CASE
            WHEN C.decimals IS NULL THEN 'false'
            ELSE 'true'
        END AS has_decimal,
        CASE
            WHEN p.price IS NULL THEN 'false'
            ELSE 'true'
        END AS has_price,
        t0._log_id,
        t0._inserted_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON t0.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            t0.block_timestamp
        ) = HOUR
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON C.address = t0.contract_address
    WHERE
        t0.block_number IN (
            SELECT
                DISTINCT t1.block_number AS block_number
            FROM
                {{ this }}
                t1
            WHERE
                t1.decimals IS NULL
                AND _inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('silver__contracts') }} C
                    WHERE
                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND C.decimals IS NOT NULL
                        AND C.address = t1.contract_address)
                )
                OR t0.block_number IN (
                    SELECT
                        DISTINCT t2.block_number
                    FROM
                        {{ this }}
                        t2
                    WHERE
                        t2.token_price IS NULL
                        AND _inserted_timestamp < (
                            SELECT
                                MAX(
                                    _inserted_timestamp
                                ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
                                AND p.token_address = t2.contract_address
                                AND p.hour = DATE_TRUNC(
                                    'hour',
                                    t2.block_timestamp
                                )
                        )
                )
        )
    {% endif %}
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        raw_amount_precise,
        raw_amount,
        amount_precise,
        amount,
        amount_usd,
        decimals,
        symbol,
        token_price,
        has_decimal,
        has_price,
        _log_id,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }} AS transfers_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        token_transfers

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount_precise,
    raw_amount,
    amount_precise_heal AS amount_precise,
    amount_heal AS amount,
    amount_usd,
    decimals,
    symbol,
    token_price,
    has_decimal,
    has_price,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    heal_model
{% endif %}
