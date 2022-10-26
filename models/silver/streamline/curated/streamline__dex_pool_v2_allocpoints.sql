{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}


with Block_number as (
select date_trunc('month',block_timestamp) as Month, min(block_number) as block_number
    from {{ ref('core__fact_blocks') }}
    group by 1
),

function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 355))
),

pool_info as (
    SELECT
        Month,
        block_number,
        '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' as contract_address,
        '0x1526fe27' AS function_signature,
       (ROW_NUMBER() over (PARTITION BY Month ORDER BY block_number)) -1 AS function_input
    FROM Block_number 
    join function_inputs
 ),


Total_alloc_point as (
    SELECT
        Month,
         block_number,
        '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' as contract_address,
        '0x17caf6f1' AS function_signature,
        NULL AS function_input
    FROM Block_number 

),


FINAL AS (
    SELECT
        Month,
        block_number,
        contract_address,
        'Pool_info_alloc_points' call_name,
        function_input,
        '0x1526fe27' AS function_signature
    FROM
        pool_info
    UNION ALL
    SELECT
        Month,
        block_number,
        contract_address,
        'Total_alloc_points' call_name,
        0 as function_input,
        '0x17caf6f1' AS function_signature
    FROM
        Total_alloc_point

)
SELECT
    concat(block_number,contract_address,function_signature,function_input) AS id,
    month,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    FINAL 
