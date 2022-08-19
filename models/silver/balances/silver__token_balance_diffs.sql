{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_timestamp::date'],
    tags = ['balances','diffs'],
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
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
update_records AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.current_bal_unadj AS balance,
        A._inserted_timestamp
    FROM
        {{ this }} A
        INNER JOIN base_table b
        ON A.address = b.address
        AND A.contract_address = b.contract_address
),
last_record AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.address,
        A.contract_address,
        A.current_bal_unadj AS balance,
        A._inserted_timestamp
    FROM
        {{ this }} A
        INNER JOIN base_table b
        ON A.address = b.address qualify(ROW_NUMBER() over (PARTITION BY A.address, A.contract_address
    ORDER BY
        A.block_number DESC)) = 1
),
all_records AS (
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        base_table
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        update_records
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        last_record
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
        all_records qualify(ROW_NUMBER() over (PARTITION BY address, contract_address, block_number
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
        {{ dbt_utils.surrogate_key(
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
    *
FROM
    FINAL
WHERE
    prev_bal_unadj <> current_bal_unadj
