{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated']
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
        COUNT(
            DISTINCT tx_hash
        ) AS transaction_count,
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
        COUNT(
            DISTINCT from_address
        ) AS unique_initiator_count,
        COUNT(
            DISTINCT to_address
        ) AS unique_to_count,
        'ETH' AS fee_currency,
        SUM(tx_fee_precise) AS total_fees,
        SUM(
            tx_fee_precise * p.price
        ) AS total_fees_usd,
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
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_initiator_count,
        'ETH' AS fee_currency,
        total_fees,
        total_fees * p.price AS total_fees_usd,
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
