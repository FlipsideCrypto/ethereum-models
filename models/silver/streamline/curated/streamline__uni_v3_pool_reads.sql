{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}
-- this model looks at the following function for uni v3 pools every 25 blocks
-- TEXT_SIGNATURE	BYTES_SIGNATURE	ID
-- protocolFees()	0x1ad8b03b	181768
-- liquidity()	0x1a686502	174769
-- feeGrowthGlobal1X128()	0x46141319	178714
-- slot0()	0x3850c7bd	178654
-- feeGrowthGlobal0X128()	0xf3058399	178713
WITH created_pools AS (

    SELECT
        created_block,
        pool_address AS contract_address
    FROM
        {{ ref('silver__uni_v3_pools') }}
),
block_range AS (
    -- edit this range to use a different block range from the ephemeral table
    SELECT
        block_number_100 AS block_input,
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
pool_block_range AS (
    -- this only includes blocks after the creation of each asset
    SELECT
        contract_address,
        block_input,
        _inserted_timestamp
    FROM
        created_pools
        JOIN block_range
    WHERE
        block_input IS NOT NULL
        AND block_input >= created_block
),
function_sigs AS (
    SELECT
        *
    FROM
        {{ ref('core__dim_function_signatures') }}
    WHERE
        id IN (
            181768,
            174769,
            178714,
            178654,
            178713
        )
),
FINAL AS (
    SELECT
        contract_address,
        block_input AS block_number,
        bytes_signature AS function_signature,
        0 AS function_input,
        _inserted_timestamp
    FROM
        pool_block_range
        JOIN function_sigs
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'uni_v3_pool_reads' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
