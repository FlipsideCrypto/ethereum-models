{# Set variables #}
{% set source_name = 'BEACON_BLOCKS' %}
{% set source_version = '' %}
{% set model_type = 'FR' %}

{%- set default_vars = fsc_evm.set_default_variables_bronze(source_name, model_type) -%}

{% set partition_function = default_vars['partition_function'] %}
{% set partition_join_key = '_partition_by_slot_id' %}
{% set balances = default_vars['balances'] %}
{% set block_number = false %}
{% set uses_receipts_by_hash = default_vars['uses_receipts_by_hash'] %}

{# Log configuration details #}
{{ fsc_evm.log_bronze_details(
    source_name = source_name,
    source_version = source_version,
    model_type = model_type,
    partition_function = partition_function,
    partition_join_key = partition_join_key,
    block_number = block_number,
    uses_receipts_by_hash = uses_receipts_by_hash
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['beacon']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query_fr(
    source_name = source_name.lower(),
    source_version = source_version.lower(),
    partition_function = partition_function,
    partition_join_key = partition_join_key,
    balances = balances,
    block_number = block_number,
    uses_receipts_by_hash = uses_receipts_by_hash
) }}
