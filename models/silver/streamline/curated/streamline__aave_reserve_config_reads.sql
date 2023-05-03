{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}
-- this model looks at the getReserveData(address) (0x35ea6a75) function for aave tokens
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
        underlying_decimals
    FROM
        {{ ref('silver__aave_tokens') }}
    WHERE
        atoken_version = 'Aave V2'
),
block_range AS (
    -- edit this range to use a different block range from the ephemeral table
    SELECT
        block_number_1000 AS block_input,
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
atoken_block_range AS (
    -- this only includes blocks after the creation of each asset
    SELECT
        atoken_address,
        atoken_version,
        atoken_created_block,
        underlying_address,
        block_input,
        _inserted_timestamp
    FROM
        atokens
        JOIN block_range
    WHERE
        block_input IS NOT NULL
        AND block_input >= atoken_created_block
),
-- see aave docs for contract addresses
data_providers AS (
    SELECT
        atoken_address,
        atoken_version,
        atoken_created_block,
        underlying_address,
        block_input,
        LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d') AS contract_address,
        _inserted_timestamp
    FROM
        atoken_block_range
),
FINAL AS (
    SELECT
        underlying_address AS function_input,
        block_input AS block_number,
        contract_address,
        '0x3e150141' AS function_signature,
        _inserted_timestamp
    FROM
        data_providers
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'aave_get_reserve_configuration' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
