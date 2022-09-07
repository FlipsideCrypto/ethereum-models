{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        -- these are the curve contract deployers, we may need to add here in the future
        from_address IN (
            '0xbabe61887f1de2713c6f97e567623453d3c79f67',
            '0x7eeac6cddbd1d0b8af061742d41877d7f707289a'
        )
        AND TYPE = 'CREATE'

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
function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 8))
),
final1 AS (
    SELECT
        block_number,
        contract_address,
        'curve_pool_token_details' AS call_name,
        _inserted_timestamp,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) -1 AS function_input
    FROM
        contract_deployments
        LEFT JOIN function_inputs
),
FINAL AS (
    SELECT
        block_number,
        contract_address,
        call_name,
        _inserted_timestamp,
        function_input,
        '0xb9947eb0' AS function_signature
    FROM
        final1
    UNION ALL
    SELECT
        block_number,
        contract_address,
        call_name,
        _inserted_timestamp,
        function_input,
        '0x87cb4f57' AS function_signature
    FROM
        final1
    UNION ALL
    SELECT
        block_number,
        contract_address,
        call_name,
        _inserted_timestamp,
        function_input,
        '0xc6610657' AS function_signature
    FROM
        final1
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
