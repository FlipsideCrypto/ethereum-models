{# Set variables #}
{%- set model_name = 'BEACON_BLOCKS' -%}
{%- set model_type = 'REALTIME' -%}

{%- set default_vars = fsc_evm.set_default_variables_streamline(model_name, model_type) -%}

{%- set node_url = default_vars['node_url'] -%}
{%- set node_secret_path = default_vars['node_secret_path'] -%}
{%- set model_quantum_state = default_vars['model_quantum_state'] -%}
{%- set testing_limit = default_vars['testing_limit'] -%}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": var((model_name ~ '_' ~ model_type ~ '_external_table').upper(), model_name.lower()),
        "sql_limit": var((model_name ~ '_' ~ model_type ~ '_sql_limit').upper(), 0),
        "producer_batch_size": var((model_name ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 0),
        "worker_batch_size": var((model_name ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 0),
        "sql_source": (model_name ~ '_' ~ model_type).lower(),
        "exploded_key": tojson(["data"]) }
    ),
    tags = ['streamline_beacon_' ~ model_type.lower()]
) }}

{# Main query starts here #}
WITH to_do AS (

    SELECT
        slot_number
    FROM
        {{ ref("streamline__beacon_blocks") }}
    WHERE
        slot_number > 5000000
        AND slot_number IS NOT NULL
    EXCEPT
    SELECT
        slot_number
    FROM
        {{ ref("streamline__beacon_blocks_complete") }}
    WHERE
        slot_number > 5000000
),
ready_slots AS (
    SELECT
        slot_number
    FROM
        to_do
    UNION
    SELECT
        slot_number
    FROM
        {{ ref("_missing_withdrawals") }}
    
    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }} 
    {% endif %}
)
SELECT
    slot_number,
    ROUND(
        slot_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'GET',
        '{{ node_url }}/eth/v2/beacon/blocks/' || slot_number,
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        NULL,
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_slots
ORDER BY
    slot_number DESC
