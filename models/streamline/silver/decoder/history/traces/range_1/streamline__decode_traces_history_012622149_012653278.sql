{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_traces(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 500000,'producer_limit_size', 10000000))", target = "{{this.schema}}.{{this.identifier}}" ),if_data_call_wait()],
    tags = ['streamline_decoded_traces_history_range_1']
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ fsc_evm.streamline_decoded_traces_requests(
    start,
    stop,
    model_type = 'history'
) }}
