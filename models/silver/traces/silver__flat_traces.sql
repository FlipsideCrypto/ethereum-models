{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE']
) }}

SELECT
    CASE
        WHEN POSITION(
            '.',
            path :: STRING
        ) > 0 THEN REPLACE(
            REPLACE(
                path :: STRING,
                SUBSTR(path :: STRING, len(path :: STRING) - POSITION('.', REVERSE(path :: STRING)) + 1, POSITION('.', REVERSE(path :: STRING))),
                ''
            ),
            '.',
            '__'
        )
        ELSE '__'
    END AS id,
    OBJECT_AGG(
        DISTINCT key,
        VALUE
    ) AS DATA,
    txs.tx_id AS tx_id,
    txs.block_id AS block_id,
    txs.block_timestamp AS block_timestamp,
    txs.ingested_at AS ingested_at,
    txs.tx :traces AS traces
FROM
    {{ ref('bronze__transactions') }}
    txs,
    TABLE(
        FLATTEN(
            input => PARSE_JSON(
                txs.tx :traces
            ),
            recursive => TRUE
        )
    ) f
WHERE
    f.index IS NULL
    AND f.key != 'calls'
    AND txs.block_timestamp > '2021-12-31'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    id,
    tx_id,
    block_id,
    block_timestamp,
    ingested_at,
    traces
