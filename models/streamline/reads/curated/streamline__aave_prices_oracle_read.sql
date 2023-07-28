{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_reads_curated']
) }}
-- this model looks at the getAssetPrice(address) (0xb3596f07) function for aave assets from the aave oracle
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
FINAL AS (
    SELECT
        underlying_address AS function_input,
        '0xb3596f07' AS function_signature,
        block_input AS block_number,
        '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' AS contract_address,
        --aave price oracle v2
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
    'aave_price_oracle' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
