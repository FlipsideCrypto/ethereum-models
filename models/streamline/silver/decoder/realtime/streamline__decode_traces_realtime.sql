{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_traces_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"DECODED_TRACES", 
        "sql_limit" :"20000000",
        "producer_batch_size" :"500000",
        "worker_batch_size" :"20000",
        "sql_source" :"{{this.identifier}}" }
    ),
    fsc_utils.if_data_call_wait()],
    tags = ['streamline_decoded_traces_realtime']
) }}
{{ fsc_evm.streamline_decoded_traces_requests(
    model_type = 'realtime'
) }}