{% macro log_complete_details(post_hook, full_refresh_type, uses_receipts_by_hash) %}

{%- if flags.WHICH == 'compile' and execute -%}

    {% if uses_receipts_by_hash %}

        {{ log("=== Current Variable Settings ===", info=True) }}
        {{ log("USES_RECEIPTS_BY_HASH: " ~ uses_receipts_by_hash, info=True) }}
    
    {% endif %}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}
    {% set config_log = config_log ~ '    cluster_by = "' ~ config.get('cluster_by') ~ '",\n' %}
    {% set config_log = config_log ~ '    merge_update_columns = ' ~ config.get('merge_update_columns') | tojson ~ ',\n' %}
    {% set config_log = config_log ~ '    post_hook = "' ~ post_hook ~ '",\n' %}
    {% set config_log = config_log ~ '    incremental_predicates = ' ~ config.get('incremental_predicates') | tojson ~ ',\n' %}
    {% set config_log = config_log ~ '    full_refresh = ' ~ full_refresh_type ~ ',\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{% endmacro %}