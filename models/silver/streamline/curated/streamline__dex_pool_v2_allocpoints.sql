{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH block_number AS (

    SELECT
        DATE_TRUNC(
            'month',
            block_timestamp
        ) AS MONTH,
        MIN(block_number) AS block_number
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp :: DATE >= '2020-02-01'

{% if is_incremental() %}
AND DATE_PART(
    DAY,
    MONTH
) = 1
AND MONTH >= (
    SELECT
        MAX(
            MONTH
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
),
function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 355))
),
pool_info AS (
    SELECT
        MONTH,
        block_number,
        '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' AS contract_address,
        '0x1526fe27' AS function_signature,
        TRIM(to_char(function_input - 1, 'XXXXXXX')) AS function_input
    FROM
        block_number
        JOIN function_inputs
),
pool_info_v2 AS (
    SELECT
        MONTH,
        block_number,
        '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' AS contract_address,
        '0x1526fe27' AS function_signature,
        TRIM(to_char(function_input - 1, 'XXXXXXX')) AS function_input
    FROM
        block_number
        JOIN function_inputs
),
lpToken_v2 AS (
    SELECT
        MONTH,
        block_number,
        '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' AS contract_address,
        '0x78ed5d1f' AS function_signature,
        TRIM(to_char(function_input - 1, 'XXXXXXX')) AS function_input
    FROM
        block_number
        JOIN function_inputs
),
total_alloc_point AS (
    SELECT
        MONTH,
        block_number,
        '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' AS contract_address,
        '0x17caf6f1' AS function_signature,
        NULL AS function_input
    FROM
        block_number
),
total_alloc_point_v2 AS (
    SELECT
        MONTH,
        block_number,
        '0xef0881ec094552b2e128cf945ef17a6752b4ec5d' AS contract_address,
        '0x17caf6f1' AS function_signature,
        NULL AS function_input
    FROM
        block_number
),
FINAL AS (
    SELECT
        MONTH,
        block_number,
        contract_address,
        'Pool_info_alloc_points' call_name,
        function_input,
        '0x1526fe27' AS function_signature
    FROM
        pool_info
    UNION ALL
    SELECT
        MONTH,
        block_number,
        contract_address,
        'Total_alloc_points' call_name,
        '' AS function_input,
        '0x17caf6f1' AS function_signature
    FROM
        total_alloc_point
),
final_v2 AS (
    SELECT
        MONTH,
        block_number,
        contract_address,
        'Pool_info_alloc_points' call_name,
        function_input,
        '0x1526fe27' AS function_signature
    FROM
        pool_info_v2
    UNION ALL
    SELECT
        MONTH,
        block_number,
        contract_address,
        'Total_alloc_points' call_name,
        '' AS function_input,
        '0x17caf6f1' AS function_signature
    FROM
        total_alloc_point_v2
    UNION ALL
    SELECT
        MONTH,
        block_number,
        contract_address,
        'lpToken()' call_name,
        function_input,
        '0x78ed5d1f' AS function_signature
    FROM
        lpToken_v2
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    MONTH,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    FINAL
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    MONTH,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    final_v2
