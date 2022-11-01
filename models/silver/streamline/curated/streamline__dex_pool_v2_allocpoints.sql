{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

with Block_number as (
select date_trunc('month',block_timestamp) as Month, min(block_number) as block_number
    from {{ ref('silver__blocks') }}
where block_timestamp::date >= '2020-02-01'
  {% if is_incremental() %}
and date_part(day, Month) = 1 
and Month >=  (
    SELECT
        MAX(
            Month
        )
    FROM
        {{ this }}
)
{% endif %}  
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
        trim(to_char(function_input - 1, 'XXXXXXX')) AS function_input
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
        '' as function_input,
        '0x17caf6f1' AS function_signature
    FROM
        Total_alloc_point

)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    month,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    FINAL 
