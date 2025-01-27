-- depends_on: {{ ref('silver__eth_balance_address_blocks') }}
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
/* NO_CACHE */
        block_number,
        block_timestamp,
        address,
        balance,
        _inserted_timestamp
    FROM
        {{ ref('silver__eth_balances') }}

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
)

{% if is_incremental() %},
all_records AS (
    -- pulls older record table
    SELECT
        block_number,
        block_timestamp,
        address,
        current_bal_unadj as balance,
        _inserted_timestamp
    FROM
        {{ ref('silver__eth_balance_address_blocks') }}
    WHERE
        address IN (
            SELECT DISTINCT address FROM base_table
        )
    UNION ALL
    -- pulls balances as usual but with only 36 hour look back to account for non-chronological blocks
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.balance,
        A._inserted_timestamp
    FROM
        {{ ref('silver__eth_balances') }} A
    WHERE
        address IN (
            SELECT
                DISTINCT address
            FROM
                base_table
        )
    AND _inserted_timestamp >= SYSDATE() - INTERVAL '25 hours'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        address,
        balance,
        _inserted_timestamp
    FROM 
       base_table
),
min_record AS (
    SELECT
        address AS min_address,
        MIN(block_number) AS min_block
    FROM
        base_table
    GROUP BY
        1
),
update_records AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        balance,
        _inserted_timestamp
    FROM
        all_records
        INNER JOIN min_record
        ON address = min_address
        AND block_number >= min_block
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        address,
        balance,
        _inserted_timestamp
    FROM
        all_records
        INNER JOIN min_record
        ON address = min_address
        AND block_number < min_block
),
incremental AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        balance,
        _inserted_timestamp
    FROM
        update_records qualify(ROW_NUMBER() over (PARTITION BY address, block_number
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        COALESCE(LAG(balance) ignore nulls over(PARTITION BY address
    ORDER BY
        block_number ASC), 0) AS prev_bal_unadj,
        balance AS current_bal_unadj,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'address']
        ) }} AS id

{% if is_incremental() %}
FROM
    incremental
{% else %}
FROM
    base_table
{% endif %}
)
SELECT
    f.*,
    id AS eth_balance_diffs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL f

{% if is_incremental() %}
INNER JOIN min_record
ON address = min_address
AND block_number >= min_block
{% endif %}
WHERE
    prev_bal_unadj <> current_bal_unadj
