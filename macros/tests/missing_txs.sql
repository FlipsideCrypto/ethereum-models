{% macro missing_txs(
        model
    ) %}
    WITH txs_base AS (
        SELECT
            block_number AS base_block_number,
            tx_hash AS base_tx_hash
        FROM
            {{ ref('test_silver__transactions_full') }}
    ),
    model_name AS (
        SELECT
            block_number AS model_block_number,
            tx_hash AS model_tx_hash
        FROM
            {{ model }}
    )
SELECT
    base_block_number,
    base_tx_hash,
    model_block_number,
    model_tx_hash
FROM
    txs_base
    LEFT JOIN model_name
    ON base_block_number = model_block_number
    AND base_tx_hash = model_tx_hash
WHERE
    base_block_number NOT IN (SELECT block_number FROM {{ref('silver_observability__excluded_trace_blocks')}})
    AND (model_tx_hash IS NULL
    OR model_block_number IS NULL)

{% endmacro %}

{% macro recent_missing_txs(
        model
    ) %}
    WITH txs_base AS (
        SELECT
            block_number AS base_block_number,
            tx_hash AS base_tx_hash
        FROM
            {{ ref('test_silver__transactions_recent') }}
    ),
    model_name AS (
        SELECT
            block_number AS model_block_number,
            tx_hash AS model_tx_hash
        FROM
            {{ model }}
    )
SELECT
    base_block_number,
    base_tx_hash,
    model_block_number,
    model_tx_hash
FROM
    txs_base
    LEFT JOIN model_name
    ON base_block_number = model_block_number
    AND base_tx_hash = model_tx_hash
WHERE
    model_tx_hash IS NULL
    OR model_block_number IS NULL
{% endmacro %}

{% macro missing_confirmed_txs(
        model1,
        model2
    ) %}
    WITH txs_base AS (
        SELECT
            block_number AS base_block_number,
            block_hash AS base_block_hash,
            tx_hash AS base_tx_hash
        FROM
            {{ model1 }}
    ),
    model_name AS (
        SELECT
            block_number AS model_block_number,
            block_hash AS model_block_hash,
            tx_hash AS model_tx_hash
        FROM
            {{ model2 }}
    )
SELECT
    DISTINCT base_block_number AS block_number
FROM
    txs_base
    LEFT JOIN model_name
    ON base_block_number = model_block_number
    AND base_tx_hash = model_tx_hash
    AND base_block_hash = model_block_hash
WHERE
    model_tx_hash IS NULL
    AND model_block_number <= (
        SELECT
            MAX(base_block_number)
        FROM
            txs_base
    )
{% endmacro %}
