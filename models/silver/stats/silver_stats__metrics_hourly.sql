{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated','reorg']
) }}

WITH new_records AS (

    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        MIN(block_number) AS block_number_min,
        MAX(block_number) AS block_number_max,
        COUNT(
            DISTINCT block_number
        ) AS block_count,
        block_count / 360 AS blocks_per_second,
        COUNT(
            DISTINCT tx_hash
        ) AS transaction_count,
        transaction_count / 360 AS transactions_per_second,
        COUNT(
            DISTINCT CASE
                WHEN tx_success THEN tx_hash
            END
        ) AS transaction_count_success,
        COUNT(
            DISTINCT CASE
                WHEN NOT tx_success THEN tx_hash
            END
        ) AS transaction_count_failed,
        (
            transaction_count_success / transaction_count
        ) AS transaction_success_rate,
        (transaction_count_success) / 360 AS successful_transactions_per_second,
        COUNT(
            DISTINCT from_address
        ) AS unique_from_count,
        COUNT(
            DISTINCT to_address
        ) AS unique_to_count,
        SUM(tx_fee_precise) AS total_fees,
        SUM(
            tx_fee_precise * p.price
        ) AS total_fees_usd,
        AVG(tx_fee_precise) AS avg_transaction_fee,
        AVG(
            tx_fee_precise * p.price
        ) AS avg_transaction_fee_usd,
        AVG(gas_price) AS avg_gas_price,
        AVG(gas_used) AS avg_gas_used,
        CASE
            WHEN p.price IS NULL THEN TRUE
            ELSE FALSE
        END AS missing_price
    FROM
        {{ ref('silver__transactions') }}
        t
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON DATE_TRUNC(
            'hour',
            t.block_timestamp
        ) = p.hour
        AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
    WHERE
        block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
)

{% if is_incremental() %},
heal_records AS (
    SELECT
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        blocks_per_second,
        transaction_count,
        transactions_per_second,
        transaction_count_success,
        transaction_count_failed,
        transaction_success_rate,
        successful_transactions_per_second,
        unique_from_count,
        unique_to_count,
        total_fees,
        total_fees * p.price AS total_fees_usd,
        avg_transaction_fee,
        avg_transaction_fee * p.price AS avg_transaction_fee_usd,
        avg_gas_price,
        avg_gas_used,
        missing_price
    FROM
        {{ this }}
    WHERE
        missing_price
)
{% endif %}
SELECT
    *
FROM
    new_records

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    heal_records
{% endif %}
