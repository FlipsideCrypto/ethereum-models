{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}
-- this looks at the decimals(), name(), and symbol() functions for all tokens used ina univ3 pool
-- it will only look at each token once
-- functions: decimals(0x313ce567), name(0x06fdde03), and symbol(0x95d89b41)
WITH uni_created_pools AS (

    SELECT
        block_number,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        AND contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'

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
all_tokens AS (
    SELECT
        block_number,
        token0 AS contract_address
    FROM
        uni_created_pools
    UNION ALL
    SELECT
        block_number,
        token1 AS contract_address
    FROM
        uni_created_pools
),
min_block AS (
    SELECT
        contract_address,
        MIN(block_number) AS block_number
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
        {{ ref('core__dim_function_signatures') }}
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
        0 AS function_input
    FROM
        min_block
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
    'uni_token_reads' AS call_name,
    SYSDATE() AS _inserted_timestamp
FROM
    FINAL
