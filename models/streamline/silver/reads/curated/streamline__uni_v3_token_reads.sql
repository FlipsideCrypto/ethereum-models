{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_reads_curated']
) }}
-- this looks at the decimals(), name(), and symbol() functions for all tokens used ina univ3 pool
-- it will only look at each token once
-- functions: decimals(0x313ce567), name(0x06fdde03), and symbol(0x95d89b41)
WITH all_tokens AS (

    SELECT
        created_block AS block_number,
        token0_address AS contract_address
    FROM
        {{ ref('silver__univ3_pools') }}
    UNION ALL
    SELECT
        created_block AS block_number,
        token1_address AS contract_address
    FROM
        {{ ref('silver__univ3_pools') }}
    UNION ALL
    SELECT
        block_number,
        token_address AS contract_address
    FROM
        {{ ref('streamline__price_api_token_reads') }}
),
min_block AS (
    SELECT
        contract_address,
        MIN(block_number) + 1 AS block_number
    FROM
        all_tokens

{% if is_incremental() %}
WHERE
    contract_address NOT IN (
        SELECT
            DISTINCT contract_address
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    contract_address
),
function_sigs AS (
    SELECT
        *
    FROM
        {{ source('ethereum_silver', 'signatures_backfill') }}
    WHERE
        id IN (
            4821,
            4820,
            4827
        )
),
FINAL AS (
    SELECT
        block_number,
        contract_address,
        bytes_signature AS function_signature,
        NULL AS function_input,
        0 AS function_input_plug
    FROM
        min_block
        JOIN function_sigs
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input_plug']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'uni_v3_token_reads' AS call_name,
    SYSDATE() AS _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
