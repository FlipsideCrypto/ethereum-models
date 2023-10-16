{{ config (
    materialized = "view",
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
    
