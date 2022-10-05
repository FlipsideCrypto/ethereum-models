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
            '0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
            '0xf18056bbd320e96a48e3fbf8bc061322531aac99',
            '0xc447fcaf1def19a583f97b3620627bf69c05b5fb',
            '0xb9fc157394af804a3578134a6585c0dc9cc990d4',
            '0xfd6f33a0509ec67defc500755322abd9df1bd5b8',
            '0xbf7d65d769e82e7b862df338223263ba33f72623',
            '0xa6df4fcb1ca559155a678e9aff5de3f210c0ff84',
            '0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
            '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c',
            '0x3e0139ce3533a42a7d342841aee69ab2bfee1d51',
            '0xbabe61887f1de2713c6f97e567623453d3c79f67',
            '0x7f7abe23fc1ad4884b726229ceaafb1179e9c9cf'
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
