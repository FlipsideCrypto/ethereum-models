{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH relevant_functions AS (
    -- pulls the following functions:
    -- exchangeRateStored, totalReserves, totalBorrows,
    -- totalSupply, supplyRatePerBlock and borrowRatePerBlock

    SELECT
        *
    FROM
        {{ ref('core__dim_function_signatures') }}
    WHERE
        id IN (
            179,
            163256,
            165096,
            165099,
            163250,
            163249
        )
),
ctokens AS (
    -- get ctoken addresses and creation dates
    SELECT
        ctoken_address AS contract_address,
        created_block
    FROM
        {{ ref('compound__ez_asset_details') }}
),
functions_join AS (
    SELECT
        contract_address,
        created_block,
        bytes_signature
    FROM
        ctokens
        JOIN relevant_functions
),
block_range AS (
    SELECT
        block_number,
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
) -- only includes blocks beyond created date for each cToken
SELECT
    0 AS function_input,
    bytes_signature AS function_signature,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    block_number,
    contract_address,
    'compound_market_stats' AS call_name,
    _inserted_timestamp
FROM
    block_range
    JOIN functions_join
WHERE
    block_number >= created_block qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
