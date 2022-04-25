{{ config(
    materialized = 'incremental',
    unique_key = '_call_id',
    cluster_by = ['ingested_at::DATE']
) }}

WITH base_table AS (

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
        ethereum_dev.silver.traces_test_data txs,
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
        id,
        tx_id,
        block_id,
        block_timestamp,
        ingested_at,
        traces
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
        ) AS eth_value,*
    FROM
        base_table
),
FINAL AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        from_address,
        gas,
        gas_used,
        input,
        output,
        TIME,
        to_address,
        TYPE,
        eth_value,
        CASE
            WHEN id = '__' THEN CONCAT(
                TYPE,
                '_ORIGIN'
            )
            ELSE CONCAT(
                TYPE,
                '_',
                REPLACE(
                    REPLACE(REPLACE(REPLACE(id, 'calls', ''), '[', ''), ']', ''),
                    '__',
                    '_'
                )
            )
        END AS identifier,
        ingested_at
    FROM
        flattened_traces
)
SELECT
    tx_id,
    block_id,
    block_timestamp,
    from_address,
    to_address,
    TYPE,
    gas,
    gas_used,
    eth_value,
    input,
    output,
    identifier,
    ingested_at,
    concat_ws(
        '-',
        tx_id,
        identifier
    ) AS _call_id
FROM
    FINAL
