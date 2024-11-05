{% macro decoded_traces_history(
        backfill_mode = false
    ) %}
    {%- set params ={ "sql_limit": var(
        "DECODED_TRACES_HISTORY_SQL_LIMIT",
        1000000
    ),
    "producer_batch_size": var(
        "DECODED_TRACES_HISTORY_PRODUCER_BATCH_SIZE",
        400000
    ),
    "worker_batch_size": var(
        "DECODED_TRACES_HISTORY_WORKER_BATCH_SIZE",
        200000
    ),
    "lookback_days": var(
        "DECODED_TRACES_HISTORY_LOOKBACK_DAYS",
        10
    ) } -%}
    {% set wait_time = var(
        "DECODED_TRACES_HISTORY_WAIT_TIME",
        60
    ) %}
    {% set find_months_query %}
    WITH exclusions AS (
        SELECT
            _call_id
        FROM
            {{ ref('streamline__complete_decoded_traces') }}
    )
SELECT
    DISTINCT DATE_TRUNC(
        'month',
        b.block_timestamp
    ) :: DATE AS MONTH
FROM
    {{ ref('core__fact_blocks') }}
    b
    INNER JOIN {{ ref('silver__traces') }}
    t
    ON t.block_number = b.block_number
    INNER JOIN {{ ref('silver__flat_function_abis') }}
    f
    ON t.to_address = f.contract_address
    AND LEFT(
        input,
        10
    ) = LEFT(
        f.function_signature,
        10
    )
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            exclusions e
        WHERE
            t._call_id = e._call_id
    ) {% if not backfill_mode %}
        AND f._inserted_timestamp > DATEADD('day',- {{ params.lookback_days }}, SYSDATE())
    {% endif %}
ORDER BY
    MONTH ASC {% endset %}
    {% set results = run_query(find_months_query) %}
    {% if execute %}
        {% set months = results.columns [0].values() %}
        {% for month in months %}
            {% set view_name = 'decoded_traces_history_' ~ month.strftime('%Y_%m') %}
            {% set create_view_query %}
            CREATE
            OR REPLACE VIEW streamline.{{ view_name }} AS (
                WITH target_blocks AS (
                    SELECT
                        block_number
                    FROM
                        {{ ref('core__fact_blocks') }}
                    WHERE
                        DATE_TRUNC(
                            'month',
                            block_timestamp
                        ) = '{{month}}' :: TIMESTAMP
                ),
                new_abis AS (
                    SELECT
                        contract_address,
                        abi,
                        function_signature,
                        function_name
                    FROM
                        {{ ref('silver__flat_function_abis') }}

                        {% if not backfill_mode %}
                        WHERE
                            _inserted_timestamp > DATEADD('day',- {{ params.lookback_days }}, SYSDATE())
                        {% endif %}),
                        abi_blocks AS (
                            SELECT
                                DISTINCT block_number
                            FROM
                                ethereum.silver.traces t
                                INNER JOIN new_abis n
                                ON t.to_address = n.contract_address
                            WHERE
                                DATE_TRUNC(
                                    'month',
                                    t.block_timestamp
                                ) = '{{month}}' :: TIMESTAMP
                        ),
                        existing_traces_to_exclude AS (
                            SELECT
                                _call_id
                            FROM
                                {{ ref('streamline__complete_decoded_traces') }}
                                INNER JOIN target_blocks USING (block_number)
                        ),
                        raw_traces AS (
                            SELECT
                                block_number,
                                tx_hash,
                                trace_index,
                                from_address,
                                to_address,
                                TYPE,
                                REGEXP_REPLACE(
                                    identifier,
                                    '[A-Z]+_',
                                    ''
                                ) AS trace_address,
                                sub_traces,
                                CASE
                                    WHEN sub_traces > 0
                                    AND trace_address = 'ORIGIN' THEN 'ORIGIN'
                                    WHEN sub_traces > 0
                                    AND trace_address != 'ORIGIN' THEN trace_address || '_'
                                    ELSE NULL
                                END AS parent_of,
                                IFF(REGEXP_REPLACE(trace_address, '.$', '') = '', 'ORIGIN', REGEXP_REPLACE(trace_address, '.$', '')) AS child_of,
                                input,
                                output,
                                _call_id
                            FROM
                                {{ ref('silver__traces') }}
                                t
                                INNER JOIN target_blocks USING (block_number)
                            WHERE
                                DATE_TRUNC(
                                    'month',
                                    t.block_timestamp
                                ) = '{{month}}' :: TIMESTAMP
                                AND EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        abi_blocks b
                                    WHERE
                                        t.block_number = b.block_number
                                )
                        ),
                        PARENT AS (
                            SELECT
                                tx_hash,
                                parent_of AS child_of,
                                input
                            FROM
                                raw_traces
                            WHERE
                                sub_traces > 0
                        ),
                        effective_contract AS (
                            SELECT
                                tx_hash,
                                TYPE AS child_type,
                                to_address AS child_to_address,
                                child_of AS parent_of,
                                input
                            FROM
                                raw_traces t
                                INNER JOIN PARENT USING (
                                    tx_hash,
                                    child_of,
                                    input
                                )
                            WHERE
                                TYPE = 'DELEGATECALL' qualify ROW_NUMBER() over (
                                    PARTITION BY t.tx_hash,
                                    t.child_of
                                    ORDER BY
                                        t.trace_index ASC
                                ) = 1
                        ),
                        final_traces AS (
                            SELECT
                                block_number,
                                tx_hash,
                                trace_index,
                                from_address,
                                to_address,
                                TYPE,
                                trace_address,
                                sub_traces,
                                parent_of,
                                child_of,
                                input,
                                output,
                                child_type,
                                child_to_address,
                                IFF(
                                    child_type = 'DELEGATECALL'
                                    AND child_to_address IS NOT NULL,
                                    child_to_address,
                                    to_address
                                ) AS effective_contract_address,
                                _call_id
                            FROM
                                raw_traces
                                LEFT JOIN effective_contract USING (
                                    tx_hash,
                                    parent_of,
                                    input
                                )
                        )
                    SELECT
                        t.block_number,
                        t.tx_hash,
                        t.trace_index,
                        _call_id,
                        f.abi AS abi,
                        f.function_name,
                        t.effective_contract_address AS abi_address,
                        t.input,
                        COALESCE(
                            t.output,
                            '0x'
                        ) AS output
                    FROM
                        final_traces t
                        LEFT JOIN {{ ref('silver__flat_function_abis') }}
                        f
                        ON t.effective_contract_address = f.contract_address
                        AND LEFT(
                            t.input,
                            10
                        ) = LEFT(
                            f.function_signature,
                            10
                        )
                        AND f._inserted_timestamp > DATEADD('day',- {{ params.lookback_days }}, SYSDATE())
                    WHERE
                        NOT EXISTS (
                            SELECT
                                1
                            FROM
                                existing_traces_to_exclude e
                            WHERE
                                e._call_id = t._call_id
                        )
                        AND f.abi IS NOT NULL
                    LIMIT
                        {{ params.sql_limit }}
                ) {% endset %}
                {# Create the view #}
                {% do run_query(create_view_query) %}
                {{ log(
                    "Created view for month " ~ month.strftime('%Y-%m'),
                    info = True
                ) }}

                {% if var(
                        "STREAMLINE_INVOKE_STREAMS",
                        false
                    ) %}
                    {# Invoke streamline, if rows exist to decode #}
                    {% set decode_query %}
                SELECT
                    streamline.udf_bulk_decode_traces_v2(
                        PARSE_JSON(
                            $${ "external_table": "decoded_traces",
                            "producer_batch_size": {{ params.producer_batch_size }},
                            "sql_limit": {{ params.sql_limit }},
                            "sql_source": "{{view_name}}",
                            "worker_batch_size": {{ params.worker_batch_size }} }$$
                        )
                    )
                WHERE
                    EXISTS(
                        SELECT
                            1
                        FROM
                            streamline.{{ view_name }}
                        LIMIT
                            1
                    ) {% endset %}
                    {% do run_query(decode_query) %}
                    {{ log(
                        "Triggered decoding for month " ~ month.strftime('%Y-%m'),
                        info = True
                    ) }}
                    {# Call wait to avoid queueing up too many jobs #}
                    {% do run_query(
                        "call system$wait(" ~ wait_time ~ ")"
                    ) %}
                    {{ log(
                        "Completed wait after decoding for month " ~ month.strftime('%Y-%m'),
                        info = True
                    ) }}
                {% endif %}
                {% endfor %}
                {% endif %}
{% endmacro %}
