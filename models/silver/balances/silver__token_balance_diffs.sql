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
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.balance,
        A._inserted_timestamp
    FROM
        {{ ref('silver__token_balances') }} A
    WHERE
        address IN (
            SELECT
                DISTINCT address
            FROM
                base_table
        )
),
min_record AS (
    SELECT
        address AS min_address,
        contract_address AS min_contract,
        MIN(block_number) AS min_block
    FROM
        base_table
    GROUP BY
        1,
        2
),
update_records AS (
    -- this gets anything in the incremental or anything newer than records in the
    -- incremental from that address already in the table
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        all_records
        INNER JOIN min_record
        ON address = min_address
        AND contract_address = min_contract
        AND block_number >= min_block
    UNION ALL
        -- old records that are not in the incremental
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        all_records
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
    FROM
        update_records qualify(ROW_NUMBER() over (PARTITION BY address, contract_address, block_number
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        COALESCE(LAG(balance) ignore nulls over(PARTITION BY address, contract_address
    ORDER BY
        block_number ASC), 0) AS prev_bal_unadj,
        balance AS current_bal_unadj,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'contract_address', 'address']
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
    id AS token_balance_diffs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL f

{% if is_incremental() %}
INNER JOIN min_record
ON address = min_address
AND contract_address = min_contract
AND block_number >= min_block
{% endif %}
WHERE
    current_bal_unadj <> prev_bal_unadj -- this inner join filters out any records that are not in the incremental
