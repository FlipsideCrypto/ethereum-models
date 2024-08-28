{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"confirm_blocks_v2",
        "sql_limit" :"5000000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_core_realtime']
) }}
{{ fsc_evm.streamline_core_requests(
    realtime = true,
    confirmed_blocks = true,
    vault_secret_path = "vault/prod/ethereum/quicknode/mainnet",
    query_limit = 600
) }}
