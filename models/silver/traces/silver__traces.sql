{{ config(
    materialized = 'incremental',
    unique_key = '_call_id',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['core'],
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
    _inserted_timestamp DESC
LIMIT
    200000
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
        _inserted_timestamp DESC)) = 1
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
        txs.ingested_at AS ingested_at,
        txs._inserted_timestamp AS _inserted_timestamp
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
        _inserted_timestamp,
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
                flattened_traces._inserted_timestamp AS _inserted_timestamp,
                flattened_traces.data AS DATA,
                flattened_traces.tx_status AS tx_status,
                group_sub_traces.sub_traces AS sub_traces
            FROM
                flattened_traces
                LEFT OUTER JOIN group_sub_traces
                ON flattened_traces.tx_hash = group_sub_traces.tx_hash
                AND flattened_traces.level = group_sub_traces.parent_level
        )
    
        final_with_traces as (
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
        case 
            when identifier in ( 'CALL_ORIGIN', 'CREATE_ORIGIN')
                then null 
            else SPLIT(
                identifier,
                '_'
                ) 
        
        end AS split_id,
        iff(split_id[1] is null, 0, split_id [1] :: INTEGER + 1) AS level1,
        iff(split_id[2] is null, 0, split_id [2] :: INTEGER + 1) AS level2,
        iff(split_id[3] is null, 0, split_id [3] :: INTEGER + 1) AS level3,
        iff(split_id[4] is null, 0, split_id [4] :: INTEGER + 1) AS level4,
        iff(split_id[5] is null, 0, split_id [5] :: INTEGER + 1) AS level5,
        iff(split_id[6] is null, 0, split_id [6] :: INTEGER + 1) AS level6,
        iff(split_id[7] is null, 0, split_id [7] :: INTEGER + 1) AS level7,
        iff(split_id[8] is null, 0, split_id [8] :: INTEGER + 1) AS level8,
        iff(split_id[9] is null, 0, split_id [9] :: INTEGER + 1) AS level9,
        iff(split_id[10] is null, 0, split_id [10] :: INTEGER + 1) AS level10,
        iff(split_id[11] is null, 0, split_id [11] :: INTEGER + 1) AS level11,
        iff(split_id[12] is null, 0, split_id [12] :: INTEGER + 1) AS level12,
        iff(split_id[13] is null, 0, split_id [13] :: INTEGER + 1) AS level13,
        iff(split_id[14] is null, 0, split_id [14] :: INTEGER + 1) AS level14,
        iff(split_id[15] is null, 0, split_id [15] :: INTEGER + 1) AS level15,
        iff(split_id[16] is null, 0, split_id [16] :: INTEGER + 1) AS level16,
        iff(split_id[17] is null, 0, split_id [17] :: INTEGER + 1) AS level17,
        iff(split_id[18] is null, 0, split_id [18] :: INTEGER + 1) AS level18,
        iff(split_id[18] is null, 0, split_id [19] :: INTEGER + 1) AS level19,
        iff(split_id[20] is null, 0, split_id [20] :: INTEGER + 1) AS level20,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                level1 ASC,
                level2 ASC,
                level3 ASC,
                level4 ASC,
                level5 ASC,
                level6 ASC,
                level7 ASC,
                level8 ASC,
                level9 ASC,
                level10 ASC,
                level11 ASC,
                level12 ASC,
                level13 ASC,
                level14 ASC,
                level15 ASC,
                level16 ASC,
                level17 ASC,
                level18 ASC,
                level19 ASC,
                level20 ASC
        ) AS traces_index,
        _call_id,
        ingested_at,
        _inserted_timestamp,
        DATA,
        tx_status,
        sub_traces
    FROM
        FINAL qualify(ROW_NUMBER() over(PARTITION BY _call_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
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
        traces_index,
        _call_id,
        ingested_at,
        _inserted_timestamp,
        DATA,
        tx_status,
        sub_traces
        from final_with_traces
