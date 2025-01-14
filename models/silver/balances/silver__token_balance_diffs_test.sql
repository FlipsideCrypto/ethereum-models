-- depends_on: {{ ref('silver__frequency_calssification_table') }}
-- depends_on: {{ ref('silver__token_balances') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

WITH base_table AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_balances') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp)
        FROM {{ this }}
    )
{% endif %}
)

{% if is_incremental() %},
last_updates AS (
    -- Get the last update timestamp for each address/contract pair
    SELECT 
        address,
        contract_address,
        MAX(block_timestamp) as last_update_time
    FROM {{ ref('silver__token_balances') }}
    WHERE (address, contract_address) IN (
        SELECT DISTINCT address, contract_address 
        FROM base_table
    )
    GROUP BY 1, 2
),

active_pairs AS (
    SELECT DISTINCT 
        b.address,
        b.contract_address,
        lu.last_update_time,
        CASE
            WHEN lu.last_update_time >= DATEADD('day', -3, CURRENT_TIMESTAMP()) THEN 'hourly'
            WHEN lu.last_update_time >= DATEADD('day', -7, CURRENT_TIMESTAMP()) THEN 'daily'
            WHEN lu.last_update_time >= DATEADD('month', -1, CURRENT_TIMESTAMP()) THEN 'weekly'
            ELSE 'longer'
        END as update_frequency
    FROM base_table b
    LEFT JOIN last_updates lu
        ON b.address = lu.address 
        AND b.contract_address = lu.contract_address
),

all_records AS (
    -- Hourly frequency pairs (3 day lookback)
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.balance,
        A._inserted_timestamp
    FROM {{ ref('silver__token_balances') }} A
    INNER JOIN active_pairs ap
        ON A.address = ap.address 
        AND A.contract_address = ap.contract_address
    WHERE ap.update_frequency = 'hourly'
        AND A.block_timestamp >= DATEADD('day', -3, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    -- Daily frequency pairs (7 day lookback)
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.balance,
        A._inserted_timestamp
    FROM {{ ref('silver__token_balances') }} A
    INNER JOIN active_pairs ap
        ON A.address = ap.address 
        AND A.contract_address = ap.contract_address
    WHERE ap.update_frequency = 'daily'
        AND A.block_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    -- Weekly frequency pairs (1 month lookback)
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.balance,
        A._inserted_timestamp
    FROM {{ ref('silver__token_balances') }} A
    INNER JOIN active_pairs ap
        ON A.address = ap.address 
        AND A.contract_address = ap.contract_address
    WHERE ap.update_frequency = 'weekly'
        AND A.block_timestamp >= DATEADD('month', -1, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    -- Longer frequency pairs (full lookback)
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.balance,
        A._inserted_timestamp
    FROM {{ ref('silver__token_balances') }} A
    INNER JOIN active_pairs ap
        ON A.address = ap.address 
        AND A.contract_address = ap.contract_address
    WHERE ap.update_frequency = 'longer'
),

-- Rest of the CTEs remain the same
missing_records AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.balance,
        A._inserted_timestamp
    FROM {{ ref('silver__token_balances') }} A
    WHERE (A.address, A.contract_address) IN (
        SELECT DISTINCT address, contract_address 
        FROM base_table
    )
    AND (A.address, A.contract_address) NOT IN (
        SELECT DISTINCT address, contract_address 
        FROM all_records
    )
)

-- The rest of your CTEs and final SELECT remain the same

combined_records AS (
    SELECT * FROM all_records
    UNION ALL
    SELECT * FROM missing_records
),

min_record AS (
    SELECT
        address AS min_address,
        contract_address AS min_contract,
        MIN(block_number) AS min_block
    FROM base_table
    GROUP BY 1, 2
),

update_records AS (
    -- Records newer than min_block
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM combined_records  -- Changed from all_records to combined_records
    INNER JOIN min_record
        ON address = min_address
        AND contract_address = min_contract
        AND block_number >= min_block

    UNION ALL

    -- Records older than min_block
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM combined_records  -- Changed from all_records to combined_records
    INNER JOIN min_record
        ON address = min_address
        AND contract_address = min_contract
        AND block_number < min_block
),


incremental AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM update_records 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY address, contract_address, block_number
        ORDER BY _inserted_timestamp DESC
    ) = 1
)
{% endif %},

FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        COALESCE(LAG(balance) ignore nulls over(
            PARTITION BY address, contract_address
            ORDER BY block_number ASC
        ), 0) AS prev_bal_unadj,
        balance AS current_bal_unadj,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'contract_address', 'address']
        ) }} AS id

{% if is_incremental() %}
    FROM incremental
{% else %}
    FROM base_table
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