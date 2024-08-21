{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_get_eth_balances(object_construct('node_name','quicknode', 'sql_source', '{{this.identifier}}'))", target = "{{this.schema}}.{{this.identifier}}" ),if_data_call_wait()],
    tags = ['streamline_native_balances_history']
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ native_balances_history(
    start,
    stop
) }}
