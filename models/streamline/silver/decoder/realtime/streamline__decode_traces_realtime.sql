{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_traces(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 500000,'producer_limit_size', 20000000))", target = "{{this.schema}}.{{this.identifier}}" ),"call system$wait(" ~ var("WAIT", 400) ~ ")" ],
    tags = ['streamline_decoded_traces_realtime']
) }}
{{ fsc_evm.streamline_decoded_traces_requests(
    model_type = 'realtime'
) }}