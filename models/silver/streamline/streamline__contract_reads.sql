{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
) }}

WITH curve_pool_details AS (

    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__curve_pool_reads') }}

{% if is_incremental() %}
WHERE
    (
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
    )
{% endif %}
)
SELECT
    id,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input,
    _inserted_timestamp
FROM
    curve_pool_details
