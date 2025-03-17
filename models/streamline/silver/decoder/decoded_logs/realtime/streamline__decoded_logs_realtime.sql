{%- set testing_limit = var('DECODED_LOGS_REALTIME_TESTING_LIMIT', none) -%}

{%- set streamline_params = { 
    "external_table": var("DECODED_LOGS_REALTIME_EXTERNAL_TABLE", "decoded_logs"),
    "sql_limit": var("DECODED_LOGS_REALTIME_SQL_LIMIT", 10000000),
    "producer_batch_size": var("DECODED_LOGS_REALTIME_PRODUCER_BATCH_SIZE", 400000),
    "worker_batch_size": var("DECODED_LOGS_REALTIME_WORKER_BATCH_SIZE", 200000),
    "sql_source": "decoded_logs_realtime"
} -%}

{# Log configuration details #}
{{ log_model_details( 
    params = streamline_params    
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2( 
        func = 'streamline.udf_bulk_decode_logs_v2', 
        target = "{{this.schema}}.{{this.identifier}}", 
        params = {
            "external_table": streamline_params['external_table'],
            "sql_limit": streamline_params['sql_limit'],
            "producer_batch_size": streamline_params['producer_batch_size'],
            "worker_batch_size": streamline_params['worker_batch_size'],
            "sql_source": streamline_params['sql_source']
        } 
    ), 
    fsc_utils.if_data_call_wait()],
    tags = ['streamline_decoded_logs_realtime']
) }}

WITH target_blocks AS (
    SELECT
        block_number
    FROM
        {{ ref('core__fact_blocks') }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                {{ ref('_24_hour_lookback') }}
        )
),
existing_logs_to_exclude AS (
    SELECT
        _log_id
    FROM
        {{ ref('streamline__decoded_logs_complete') }}
        l
        INNER JOIN target_blocks b USING (block_number)
    WHERE
        l.inserted_timestamp :: DATE >= DATEADD('day', -2, SYSDATE())
),
candidate_logs AS (
    SELECT
        l.block_number,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        l.topics,
        l.data,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id
    FROM
        target_blocks b
        INNER JOIN {{ ref('core__fact_event_logs') }}
        l USING (block_number)
    WHERE
        l.tx_succeeded
        AND l.inserted_timestamp :: DATE >= DATEADD('day', -2, SYSDATE())
)
SELECT
    l.block_number,
    l._log_id,
    A.abi,
    OBJECT_CONSTRUCT(
        'topics',
        l.topics,
        'data',
        l.data,
        'address',
        l.contract_address
    ) AS DATA
FROM
    candidate_logs l
    INNER JOIN {{ ref('silver__complete_event_abis') }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics [0] :: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            existing_logs_to_exclude e
        WHERE
            e._log_id = l._log_id
    )

{% if testing_limit is not none %}
    LIMIT
        {{ testing_limit }}
{% endif %}