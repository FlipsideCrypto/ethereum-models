{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH relevant_functions AS (
    -- pulls the following functions:
    -- compSpeeds compBorrowSpeeds and compSupplySpeeds

    SELECT
        *
    FROM
        {{ ref('core__dim_function_signatures') }}
    WHERE
        id IN (
            171502,
            355739,
            355738
        )
),
ctokens AS (
    -- get ctoken addresses and creation dates
    SELECT
        ctoken_address AS function_input,
        created_block
    FROM
        {{ ref('compound__ez_asset_details') }}
),
functions_join AS (
    SELECT
        function_input,
        bytes_signature,
        '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b' AS contract_address,
        -- proxy '0xbafe01ff935c7305907c33bf824352ee5979b526'
        created_block
    FROM
        ctokens
        JOIN relevant_functions
),
block_range AS (
    SELECT
        block_number_25 AS block_number,
        _inserted_timestamp
    FROM
        {{ ref('_block_ranges') }}
    WHERE
        block_number > 7500000

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
        function_input,
        bytes_signature AS function_signature,
        block_number,
        contract_address,
        'compound_comptroller_stats' AS call_name,
        _inserted_timestamp,
        CASE
            WHEN bytes_signature = '0x1d7b33d7'
            AND block_number > 13500000 THEN 1
            ELSE 0
        END AS exclude_flag
    FROM
        block_range
        JOIN functions_join
    WHERE
        block_number >= created_block
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    call_name,
    _inserted_timestamp
FROM
    FINAL
WHERE
    exclude_flag = 0 qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
