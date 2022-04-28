{{ config(
    materialized = 'incremental',
    unique_key = '_call_id',
    cluster_by = ['ingested_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH new_blocks AS (

    SELECT
        block_id
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        tx_count > 0

{% if is_incremental() %}
AND block_id NOT IN (
    SELECT
        DISTINCT block_number
    FROM
        {{ this }}
)
{% endif %}
ORDER BY
    ingested_at DESC
LIMIT
    180000
), traces_txs AS (
    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        block_id IN (
            SELECT
                block_id
            FROM
                new_blocks
        )
),
base_table AS (
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
        txs.tx_id AS tx_hash,
        txs.block_id AS block_number,
        txs.block_timestamp AS block_timestamp,
        txs.ingested_at AS ingested_at
    FROM
        traces_txs txs,
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
    GROUP BY
        tx_hash,
        id,
        block_number,
        block_timestamp,
        ingested_at
),
flattened_traces AS (
    SELECT
        DATA :from :: STRING AS from_address,
        TO_NUMBER(REPLACE(DATA :gas :: STRING, '0x', ''), 'XXXXXXX') AS gas,
        TO_NUMBER(REPLACE(DATA :gas :: STRING, '0x', ''), 'XXXXXXX') AS gas_used,
        DATA :input :: STRING AS input,
        DATA :output :: STRING AS output,
        DATA :time :: STRING AS TIME,
        DATA :to :: STRING AS to_address,
        DATA :type :: STRING AS TYPE,
        silver.js_hex_to_int(
            DATA :value :: STRING
        ) / pow(
            10,
            18
        ) AS eth_value,
        CASE
            WHEN id = '__' THEN CONCAT(
                DATA :type :: STRING,
                '_ORIGIN'
            )
            ELSE CONCAT(
                DATA :type :: STRING,
                '_',
                REPLACE(
                    REPLACE(REPLACE(REPLACE(id, 'calls', ''), '[', ''), ']', ''),
                    '__',
                    '_'
                )
            )
        END AS identifier,
        concat_ws(
            '-',
            tx_hash,
            identifier
        ) AS _call_id,*
    FROM
        base_table
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    _call_id,
    ingested_at,
    DATA
FROM
    flattened_traces qualify(ROW_NUMBER() over(PARTITION BY _call_id
ORDER BY
    ingested_at DESC)) = 1
