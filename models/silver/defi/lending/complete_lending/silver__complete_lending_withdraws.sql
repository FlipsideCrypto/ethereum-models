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
        aave_token AS protocol_token,
        aave_market AS token_address,
        symbol as token_symbol,
        withdrawn_tokens_unadj AS amount_unadj,
        withdrawn_tokens AS amount,
        withdrawn_usd AS amount_usd,
        depositor_address,
        aave_version AS platform,
        blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_withdraws') }}

{% if is_incremental() and 'aave' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
radiant as (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        radiant_token AS protocol_market,
        radiant_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__radiant_withdraws') }}

    {% if is_incremental() and 'radiant' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),
spark as (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        spark_token AS protocol_market,
        spark_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__spark_withdraws') }}

    {% if is_incremental() and 'spark' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),

sturdy as (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        sturdy_token AS protocol_market,
        sturdy_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__sturdy_withdraws') }}

    {% if is_incremental() and 'sturdy' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),

uwu as (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        uwu_token AS protocol_market,
        uwu_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__uwu_withdraws') }}

    {% if is_incremental() and 'uwu' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),

cream as (
        
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        token_address AS protocol_market,
        received_contract_address AS token_address,
        received_contract_symbol token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        redeemer AS depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__cream_withdraws') }}

    {% if is_incremental() and 'cream' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),

flux as (
        
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        token_address AS protocol_market,
        received_contract_address AS token_address,
        received_contract_symbol token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        redeemer AS depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__flux_withdraws') }}

    {% if is_incremental() and 'flux' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),

strike as (
        
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        token_address AS protocol_market,
        received_contract_address AS token_address,
        received_contract_symbol token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        redeemer AS depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__strike_withdraws') }}

    {% if is_incremental() and 'strike' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
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
        ctoken AS protocol_token,
        CASE
            WHEN received_contract_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE received_contract_address
        END AS token_address,
        received_contract_symbol AS token_symbol,
        received_amount_unadj AS amount_unadj,
        received_amount AS amount,
        received_amount_usd AS amount_usd,
        redeemer AS depositor_address,
        compound_version AS platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__comp_redemptions') }}

    {% if is_incremental() and 'comp' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '12 hours'
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
        frax_market_address AS protocol_token,
        underlying_asset AS token_address,
        underlying_symbol AS token_symbol,
        withdraw_amount_unadj AS amount_unadj,
        withdraw_amount AS amount,
        NULL AS amount_usd,
        caller AS depositor_address,
        'Fraxlend' AS platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__fraxlend_withdraws') }}

    {% if is_incremental() and 'fraxlend' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
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
        silo_market AS protocol_market,
        token_address,
        token_symbol,
        amount_unadj,
        amount,
        NULL AS amount_usd,
        depositor_address,
        platform,
        'ethereum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__silo_withdraws') }}

    {% if is_incremental() and 'silo' not in var('HEAL_CURATED_MODEL') %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '36 hours'
            FROM
                {{ this }}
        )
    {% endif %}
),
withdraw_union as (
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
            WHEN platform = 'Fraxlend' THEN 'RemoveCollateral'
            WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
            WHEN platform IN (
                'Compound V2',
                'Cream',
                'Flux',
                'Strike') THEN 'Redeem'
            WHEN platform = 'Aave V1' THEN 'RedeemUnderlying'
            ELSE 'Withdraw'
        END AS event_name,
        protocol_token AS protocol_market,
        a.token_address,
        token_symbol,
        amount_unadj,
        amount,
        CASE
            WHEN platform NOT LIKE '%Aave%' OR platform NOT LIKE '%Compound%' 
            THEN ROUND((amount * p.price), 2)
            ELSE ROUND(
                amount_usd,
                2
            )
        END AS amount_usd,
        depositor_address as depositor,
        platform,
        A.blockchain,
        A._log_id,
        A._inserted_timestamp
    FROM
        withdraw_union A
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON a.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON a.token_address = C.address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
