{{ config(
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = "streamline.udf_bulk_rest_api_v2",
        target = "{{ this.schema }}.{{ this.identifier }}",
        params ={ "external_table": "blocks_v2",
        "sql_limit": "50000",
        "producer_batch_size": "1000",
        "worker_batch_size": "100",
        "sql_source": "{{ this.identifier }}",
        "exploded_key": tojson(["data", "result.transactions"]) }
    ),
    tags = ['streamline_core_realtime']
) }}
{{ fsc_evm.streamline_core_requests(
    model_type = 'realtime',
    model = 'blocks_transactions',
    quantum_state = 'streamline',
    vault_secret_path = "vault/prod/ethereum/quicknode/mainnet",
    query_limit = 300
) }}
