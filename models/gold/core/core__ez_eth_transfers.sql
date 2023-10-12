{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "{{ grant_data_share_statement('EZ_ETH_TRANSFERS', 'TABLE') }}",
    tags = ['realtime','reorg']
) }}

WITH eth_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        identifier,
        _call_id,
        _inserted_timestamp,
        input,
        eth_value_precise_raw,
        eth_value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        TYPE = 'CALL'
        AND eth_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                eth_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.from_address AS eth_from_address,
    A.to_address AS eth_to_address,
    A.eth_value AS amount,
    A.eth_value_precise_raw AS amount_precise_raw,
    A.eth_value_precise AS amount_precise,
    ROUND(
        A.eth_value * price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index
FROM
    eth_base A
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    AND token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
