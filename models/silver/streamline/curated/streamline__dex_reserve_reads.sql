{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH base AS (

    SELECT
        block_number,
        contract_address,
        _inserted_timestamp,
        CASE
            WHEN platform IN (
                'uniswap-v2',
                'sushiswap'
            ) THEN '0x0902f1ac'
            ELSE 'missing'
        END AS function_signature,
        NULL AS function_input,
        0 AS function_input_plug
    FROM
        {{ ref('core__ez_dex_swaps') }}
    WHERE
        platform IN (
            'uniswap-v2',
            'sushiswap'
        )
        AND origin_to_address = '0x00000000003b3cc22af3ae1eac0440bcee416b40'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        contract_address,
        function_signature,
        _inserted_timestamp,
        function_input,
        function_input_plug
    FROM
        base
    UNION
    SELECT
        block_number + 1 AS block_number,
        contract_address,
        function_signature,
        _inserted_timestamp,
        function_input,
        function_input_plug
    FROM
        base
    UNION
    SELECT
        block_number - 1 AS block_number,
        contract_address,
        function_signature,
        _inserted_timestamp,
        function_input,
        function_input_plug
    FROM
        base
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input_plug']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'dex_reserve_reads' AS call_name,
    _inserted_timestamp
FROM
    FINAL
WHERE
    function_signature <> 'missing' qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
