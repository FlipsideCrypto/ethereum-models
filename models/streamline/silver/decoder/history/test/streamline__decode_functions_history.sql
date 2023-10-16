{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{model.schema}}.udf_bulk_decode_functions(object_construct('sql_source', '{{model.alias}}','producer_batch_size', 20000000,'producer_limit_size', {{var('row_limit',7500000)}}))", target = "{{model.schema}}.{{model.alias}}" ) ,if_data_call_wait()],
    tags = ['streamline_decoded_functions_history']
) }}

SELECT
    l.block_number,
    l.tx_hash,
    A.abi AS abi,
    case when type = 'DELEGATECALL' then from_address 
    else to_address end as abi_address,
    l.input AS DATA
FROM
    {{ ref("silver__traces") }} l
    INNER JOIN {{ ref("silver__complete_function_abis") }} A
    on A.parent_contract_address = abi_address
WHERE
    (
        l.block_number BETWEEN 5129922
        AND 5129923
    )
