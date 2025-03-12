{% macro set_default_variables_streamline(model_name, model_type) %}

{%- set node_url = var('GLOBAL_NODE_URL', '{Service}/{Authentication}') -%}
{%- set node_secret_path = var('GLOBAL_NODE_SECRET_PATH', '') -%}
{%- set model_quantum_state = var((model_name ~ '_' ~ model_type ~ '_quantum_state').upper(), 'streamline') -%}
{%- set testing_limit = var((model_name ~ '_' ~ model_type ~ '_testing_limit').upper(), none) -%}
{%- set new_build = var((model_name ~ '_' ~ model_type ~ '_new_build').upper(), false) -%}
{%- set default_order = 'ORDER BY partition_key DESC, block_number DESC' if model_type.lower() == 'realtime' 
    else 'ORDER BY partition_key ASC, block_number ASC' -%}
{%- set order_by_clause = var((model_name ~ '_' ~ model_type ~ '_order_by_clause').upper(), default_order) -%}
{%- set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) -%}

{%- set variables = {
    'node_url': node_url,
    'node_secret_path': node_secret_path,
    'model_quantum_state': model_quantum_state,
    'testing_limit': testing_limit,
    'new_build': new_build,
    'order_by_clause': order_by_clause,
    'uses_receipts_by_hash': uses_receipts_by_hash
} -%}

{{ return(variables) }}

{% endmacro %}  

{% macro set_default_variables_bronze(source_name, model_type) %}

{%- set partition_function = var(source_name ~ model_type ~ '_PARTITION_FUNCTION', 
 "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)") 
-%}
{%- set partition_join_key = var(source_name ~ model_type ~ '_PARTITION_JOIN_KEY', 'partition_key') -%}
{%- set block_number = var(source_name ~ model_type ~ '_BLOCK_NUMBER', true) -%}
{%- set balances = var(source_name ~ model_type ~ '_BALANCES', false) -%}
{%- set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) -%}

{%- set variables = {
    'partition_function': partition_function,
    'partition_join_key': partition_join_key,
    'block_number': block_number,
    'balances': balances,
    'uses_receipts_by_hash': uses_receipts_by_hash
} -%}

{{ return(variables) }}

{% endmacro %} 