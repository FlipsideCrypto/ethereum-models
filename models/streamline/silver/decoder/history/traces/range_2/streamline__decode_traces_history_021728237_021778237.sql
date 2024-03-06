{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_traces(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 500000,'producer_limit_size', 20000000))", target = "{{this.schema}}.{{this.identifier}}" ),if_data_call_wait()],
    tags = ['streamline_decoded_traces_history_range_2']
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ decode_traces_history(
    start,
    stop
) }}
