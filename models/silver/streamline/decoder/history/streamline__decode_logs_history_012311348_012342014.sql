{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 20000000,'producer_limit_size', 20000000))", target = "{{this.schema}}.{{this.identifier}}" ),"call system$wait(" ~ var("WAIT", 400) ~ ")" ]
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ decode_logs_history(
    start,
    stop
) }}
