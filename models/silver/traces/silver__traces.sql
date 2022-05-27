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
    500000
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
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        ingested_at DESC)) = 1
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
        CASE
            WHEN txs.tx :receipt :status :: STRING = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
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
        ingested_at,
        tx_status
),
flattened_traces AS (
    SELECT
        DATA :from :: STRING AS from_address,
        TO_NUMBER(REPLACE(DATA :gas :: STRING, '0x', ''), 'XXXXXXX') AS gas,
        TO_NUMBER(REPLACE(DATA :gasUsed :: STRING, '0x', ''), 'XXXXXXX') AS gas_used,
        DATA :input :: STRING AS input,
        DATA :output :: STRING AS output,
        DATA :time :: STRING AS TIME,
        DATA :to :: STRING AS to_address,
        DATA :type :: STRING AS TYPE,
        CASE
            WHEN DATA :type :: STRING = 'CALL' THEN silver.js_hex_to_int(
                DATA :value :: STRING
            ) / pow(
                10,
                18
            )
            ELSE 0
        END AS eth_value,
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
        ) AS _call_id,
        SPLIT(
            identifier,
            '_'
        ) AS id_split,
        ARRAY_SLICE(id_split, 1, ARRAY_SIZE(id_split)) AS levels,
        ARRAY_TO_STRING(
            levels,
            '_'
        ) AS LEVEL,
        CASE
            WHEN ARRAY_SIZE(levels) = 1
            AND levels [0] :: STRING = 'ORIGIN' THEN NULL
            WHEN ARRAY_SIZE(levels) = 1 THEN 'ORIGIN'
            ELSE ARRAY_TO_STRING(ARRAY_SLICE(levels, 0, ARRAY_SIZE(levels) -1), '_')END AS parent_level,
            COUNT(parent_level) over (
                PARTITION BY tx_hash,
                parent_level
            ) AS sub_traces,*
            FROM
                base_table
        ),
        group_sub_traces AS (
            SELECT
                tx_hash,
                parent_level,
                sub_traces
            FROM
                flattened_traces
            GROUP BY
                tx_hash,
                parent_level,
                sub_traces
        ),
        FINAL AS (
            SELECT
                flattened_traces.tx_hash AS tx_hash,
                flattened_traces.block_number AS block_number,
                flattened_traces.block_timestamp AS block_timestamp,
                flattened_traces.from_address AS from_address,
                flattened_traces.to_address AS to_address,
                flattened_traces.eth_value AS eth_value,
                flattened_traces.gas AS gas,
                flattened_traces.gas_used AS gas_used,
                flattened_traces.input AS input,
                flattened_traces.output AS output,
                flattened_traces.type AS TYPE,
                flattened_traces.identifier AS identifier,
                flattened_traces._call_id AS _call_id,
                flattened_traces.ingested_at AS ingested_at,
                flattened_traces.data AS DATA,
                flattened_traces.tx_status AS tx_status,
                group_sub_traces.sub_traces AS sub_traces
            FROM
                flattened_traces
                LEFT OUTER JOIN group_sub_traces
                ON flattened_traces.tx_hash = group_sub_traces.tx_hash
                AND flattened_traces.level = group_sub_traces.parent_level
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
        DATA,
        tx_status,
        sub_traces
    FROM
        FINAL qualify(ROW_NUMBER() over(PARTITION BY _call_id
    ORDER BY
        ingested_at DESC)) = 1
