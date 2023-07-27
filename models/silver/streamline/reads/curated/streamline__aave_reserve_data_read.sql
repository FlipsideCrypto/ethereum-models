{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view','streamline_reads_curated']
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
-- see aave docs for contract addresses
data_providers AS (
    SELECT
        atoken_address,
        atoken_version,
        atoken_created_block,
        underlying_address,
        block_input,
        CASE
            WHEN atoken_version = 'Aave V2' THEN LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')
            WHEN atoken_version = 'Aave AMM' THEN LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
            WHEN atoken_version = 'Aave V1' THEN LOWER('0xc1ec30dfd855c287084bf6e14ae2fdd0246baf0d') -- v1 proxy '0x398eC7346DcD622eDc5ae82352F02bE94C62d119
        END AS contract_address,
        _inserted_timestamp
    FROM
        atoken_block_range
),
lending_pools AS (
    SELECT
        atoken_address,
        atoken_version,
        atoken_created_block,
        underlying_address,
        block_input,
        CASE
            WHEN atoken_version = 'Aave V2' THEN LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9') -- v2 proxy  0xc6845a5c768bf8d7681249f8927877efda425baf
            WHEN atoken_version = 'Aave AMM' THEN LOWER('0xaaca8859efd9643b98c042691da60b217c9cdd64') -- amm proxy 0x7937d4799803fbbe595ed57278bc4ca21f3bffcb
        END AS contract_address,
        _inserted_timestamp
    FROM
        atoken_block_range
    WHERE
        atoken_version <> 'Aave V1'
),
FINAL AS (
    SELECT
        underlying_address AS function_input,
        block_input AS block_number,
        contract_address,
        '0x35ea6a75' AS function_signature,
        _inserted_timestamp
    FROM
        data_providers
    UNION
    SELECT
        underlying_address AS function_input,
        block_input AS block_number,
        contract_address,
        '0x35ea6a75' AS function_signature,
        _inserted_timestamp
    FROM
        lending_pools
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'aave_reserve_data' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
