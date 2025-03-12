{% macro log_streamline_details(model_name, model_type, node_url, model_quantum_state, sql_limit, testing_limit, order_by_clause, new_build, streamline_params, uses_receipts_by_hash, method, method_params, min_block=0) %}

{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("START_UP_BLOCK: " ~ min_block, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== API Details ===", info=True) }}

    {{ log("NODE_URL: " ~ node_url, info=True) }}
    {{ log("NODE_SECRET_PATH: " ~ var('GLOBAL_NODE_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log((model_name ~ '_' ~ model_type ~ '_model_quantum_state').upper() ~ ': ' ~ model_quantum_state, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_sql_limit').upper() ~ ': ' ~ sql_limit, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_testing_limit').upper() ~ ': ' ~ testing_limit, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_order_by_clause').upper() ~ ': ' ~ order_by_clause, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_new_build').upper() ~ ': ' ~ new_build, info=True) }}
    {{ log('USES_RECEIPTS_BY_HASH' ~ ': ' ~ uses_receipts_by_hash, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== RPC Details ===", info=True) }}

    {{ log(model_name ~ ": {", info=True) }}
    {{ log("    method: '" ~ method ~ "',", info=True) }}
    {{ log("    method_params: " ~ method_params, info=True) }}
    {{ log("}", info=True) }}
    {{ log("", info=True) }}

    {% set params_str = streamline_params | tojson %}
    {% set params_formatted = params_str | replace('{', '{\n            ') | replace('}', '\n        }') | replace(', ', ',\n            ') %}
    
    {# Clean up the method_params formatting #}
    {% set params_formatted = params_formatted | replace('"method_params": "', '"method_params": "') | replace('\\n', ' ') | replace('\\u0027', "'") %}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    post_hook = fsc_utils.if_data_call_function_v2(\n' %}
    {% set config_log = config_log ~ '        func = "streamline.udf_bulk_rest_api_v2",\n' %}
    {% set config_log = config_log ~ '        target = "' ~ this.schema ~ '.' ~ this.identifier ~ '",\n' %}
    {% set config_log = config_log ~ '        params = ' ~ params_formatted ~ '\n' %}
    {% set config_log = config_log ~ '    ),\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{% endmacro %}