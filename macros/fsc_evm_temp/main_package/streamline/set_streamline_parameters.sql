{% macro set_streamline_parameters(model_name, model_type, multiplier=1) %}

{%- set rpc_config_details = {
    "blocks_transactions": {
        "method": 'eth_getBlockByNumber',
        "method_params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)',
        "exploded_key": ['result', 'result.transactions']
    },
    "receipts_by_hash": {
        "method": 'eth_getTransactionReceipt',
        "method_params": 'ARRAY_CONSTRUCT(tx_hash)'
    },
    "receipts": {
        "method": 'eth_getBlockReceipts',
        "method_params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))',
        "exploded_key": ['result'],
        "lambdas": 2

    },
    "traces": {
        "method": 'debug_traceBlockByNumber',
        "method_params": "ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))",
        "exploded_key": ['result'],
        "lambdas": 2
    },
    "confirm_blocks": {
        "method": 'eth_getBlockByNumber',
        "method_params": 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)'
    }
} -%}

{%- set rpc_config = rpc_config_details[model_name.lower()] -%}

{%- set params = {
    "external_table": var((model_name ~ '_' ~ model_type ~ '_external_table').upper(), model_name.lower()),
    "sql_limit": var((model_name ~ '_' ~ model_type ~ '_sql_limit').upper(), 2 * var('GLOBAL_BLOCKS_PER_HOUR',0) * multiplier),
    "producer_batch_size": var((model_name ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 2 * var('GLOBAL_BLOCKS_PER_HOUR',0) * multiplier),
    "worker_batch_size": var(
        (model_name ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 
        (2 * var('GLOBAL_BLOCKS_PER_HOUR',0) * multiplier) // (rpc_config.get('lambdas', 1))
    ),
    "sql_source": (model_name ~ '_' ~ model_type).lower(),
    "method": rpc_config['method'],
    "method_params": rpc_config['method_params']
} -%}

{%- if rpc_config.get('exploded_key') is not none -%}
    {%- do params.update({"exploded_key": tojson(rpc_config['exploded_key'])}) -%}
{%- endif -%}

{%- if rpc_config.get('lambdas') is not none -%}
    {%- do params.update({"lambdas": rpc_config['lambdas']}) -%}
{%- endif -%}

{{ return(params) }}

{% endmacro %}



