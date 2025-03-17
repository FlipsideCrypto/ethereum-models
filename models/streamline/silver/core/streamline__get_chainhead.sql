{%- set model_quantum_state = var('CHAINHEAD_QUANTUM_STATE', 'livequery') -%}

{%- set node_url = var('GLOBAL_NODE_URL', '{Service}/{Authentication}') -%}

{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("CHAINHEAD_QUANTUM_STATE: " ~ model_quantum_state, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== API Details ===", info=True) }}
    {{ log("NODE_URL: " ~ node_url, info=True) }}
    {{ log("NODE_SECRET_PATH: " ~ var('GLOBAL_NODE_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{{ config (
    materialized = 'table',
    tags = ['streamline_core_complete','chainhead']
) }}

SELECT
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        '{{ var('GLOBAL_NODE_SECRET_PATH') }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number