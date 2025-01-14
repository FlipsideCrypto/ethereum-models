{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

WITH base_records AS (
    -- Use MATCH_RECOGNIZE right at the start to identify only records where balances change
    SELECT
        address,
        contract_address,
        block_number,
        block_timestamp,
        prev_bal_unadj,
        current_bal_unadj,
        _inserted_timestamp
    FROM {{ ref('silver__token_balances') }}
    MATCH_RECOGNIZE(
        PARTITION BY address, contract_address
        ORDER BY block_number
        MEASURES
            LAG(balance) AS prev_bal_unadj,
            balance AS current_bal_unadj,
            block_number AS block_number,
            block_timestamp AS block_timestamp,
            _inserted_timestamp AS _inserted_timestamp
        ONE ROW PER MATCH
        PATTERN (balance_change)
        DEFINE
            -- Only match when balance actually changes (or it's the first record)
            balance_change AS 
                CASE 
                    WHEN LAG(balance) IS NULL THEN balance != 0
                    ELSE balance != LAG(balance)
                END
    )
    {% if is_incremental() %}
    WHERE _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

{% if is_incremental() %}
min_record AS (
    SELECT
        address AS min_address,
        contract_address AS min_contract,
        MIN(block_number) AS min_block
    FROM base_records
    GROUP BY 1, 2
),

relevant_history AS (
    -- Get only the relevant historical records where balances changed
    SELECT 
        address,
        contract_address,
        block_number,
        block_timestamp,
        prev_bal_unadj,
        current_bal_unadj,
        _inserted_timestamp
    FROM {{ ref('silver__token_balances') }}
    MATCH_RECOGNIZE(
        PARTITION BY address, contract_address
        ORDER BY block_number
        MEASURES
            LAG(balance) AS prev_bal_unadj,
            balance AS current_bal_unadj,
            block_number AS block_number,
            block_timestamp AS block_timestamp,
            _inserted_timestamp AS _inserted_timestamp
        ONE ROW PER MATCH
        PATTERN (balance_change)
        DEFINE
            balance_change AS 
                CASE 
                    WHEN LAG(balance) IS NULL THEN balance != 0
                    ELSE balance != LAG(balance)
                END
    )
    WHERE (address, contract_address) IN (
        SELECT DISTINCT address, contract_address 
        FROM base_records
    )
)
{% endif %},

FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        prev_bal_unadj,
        current_bal_unadj,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'contract_address', 'address']
        ) }} AS id
    {% if is_incremental() %}
    FROM relevant_history
    {% else %}
    FROM base_records
    {% endif %}
)

SELECT
    f.*,
    id AS token_balance_diffs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM FINAL f
{% if is_incremental() %}
INNER JOIN min_record
    ON address = min_address
    AND contract_address = min_contract
    AND block_number >= min_block
{% endif %}
WHERE current_bal_unadj <> prev_bal_unadj