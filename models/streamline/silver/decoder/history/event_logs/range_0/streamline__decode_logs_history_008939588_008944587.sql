{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{model.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{model.alias}}','producer_batch_size', 20000000,'producer_limit_size', 500000))", target = "{{model.schema}}.{{model.alias}}" ) ,if_data_call_wait()]
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ fsc_evm.streamline_decoded_logs_requests(
    start,
    stop,
    model_type = 'history'
) }}
