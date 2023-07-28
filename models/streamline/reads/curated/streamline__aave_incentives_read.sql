{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view','streamline_reads_curated']
) }}
-- this model looks at the aave incentives controller (0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5) for the following functions:
-- assets(address) 0xf11b8188 - the input here is the atoken address
-- NOTE we need to input the debt position tokens here as well. The best place to get them is from the output of the aave reserve read
-- TO DO switch this to the reserve read when data is available - we are missing some here currently
WITH atokens AS (

    SELECT
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        atoken_version,
        atoken_created_block,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__aave_tokens') }}
    WHERE
        atoken_version <> 'v1'
),
block_range AS (
    -- edit this range to use a different block range from the ephemeral table
    SELECT
        block_number_25 AS block_input,
        _inserted_timestamp
    FROM
        {{ ref('_block_ranges') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
addresses AS (
    SELECT
        atoken_address AS function_input,
        atoken_created_block
    FROM
        atokens
    UNION
    SELECT
        atoken_stable_debt_address AS function_input,
        atoken_created_block
    FROM
        atokens
    UNION
    SELECT
        atoken_variable_debt_address AS function_input,
        atoken_created_block
    FROM
        atokens
),
atoken_block_range AS (
    -- this only includes blocks after the creation of each asset
    SELECT
        function_input,
        _inserted_timestamp,
        block_input
    FROM
        addresses
        JOIN block_range
    WHERE
        block_input IS NOT NULL
        AND block_input >= atoken_created_block
),
FINAL AS (
    SELECT
        function_input,
        '0xf11b8188' AS function_signature,
        block_input AS block_number,
        '0xd9ed413bcf58c266f95fe6ba63b13cf79299ce31' AS contract_address,
        --aave incentives controller
        -- proxy       '0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5'
        _inserted_timestamp
    FROM
        atoken_block_range
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'aave_incentives_controller' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
