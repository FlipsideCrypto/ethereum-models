{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_date'],
    tags = ['balances'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH

{% if is_incremental() %}
control_incremental AS (

    SELECT
        MAX(
            block_date
        ) :: DATE AS min_inc_block_date,
        DATEADD(
            'day',
            60,
            min_inc_block_date
        ) :: DATE AS inc_block_date
    FROM
        {{ this }}
),
load_records_incremental AS (
    SELECT
        block_date,
        address,
        balance,
        _inserted_timestamp
    FROM
        {{ ref('silver__eth_balances') }}
    WHERE
        (
            block_date > (
                SELECT
                    min_inc_block_date
                FROM
                    control_incremental
            )
        )
        AND (
            block_date <= (
                SELECT
                    inc_block_date
                FROM
                    control_incremental
            )
        )
),
recent_records AS (
    SELECT
        block_date,
        address,
        balance,
        _inserted_timestamp
    FROM
        {{ this }}
    WHERE
        block_date :: DATE = (
            SELECT
                min_inc_block_date
            FROM
                control_incremental
        )
),
all_incremental_records AS (
    SELECT
        block_date,
        address,
        balance,
        _inserted_timestamp,
        1 AS RANK
    FROM
        load_records_incremental
    UNION
    SELECT
        block_date,
        address,
        balance,
        _inserted_timestamp,
        2 AS RANK
    FROM
        recent_records
),
incremental AS (
    SELECT
        block_date,
        address,
        balance,
        _inserted_timestamp
    FROM
        all_incremental_records qualify(ROW_NUMBER() over(PARTITION BY address, block_date
    ORDER BY
        RANK ASC)) = 1
),
{% else %}
    fr_balances AS (
        SELECT
            block_date,
            address,
            balance,
            _inserted_timestamp
        FROM
            {{ ref('silver__eth_balances') }}
        WHERE
            block_date < '2017-04-30' -- block_date BETWEEN '2021-01-01'
            -- AND '2021-01-31'
            -- if we backfill further and want to FR this table, will need to tweak this filter
            qualify(ROW_NUMBER() over(PARTITION BY address, block_date
        ORDER BY
            _inserted_timestamp DESC)) = 1
    ),
{% endif %}

info AS (

{% if is_incremental() %}
SELECT
    MAX(block_date) :: DATE AS max_block, MIN(block_date) :: DATE AS min_block
FROM
    incremental
{% else %}
SELECT
    MAX(block_date) :: DATE AS max_block, MIN(block_date) :: DATE AS min_block
FROM
    fr_balances
{% endif %}),
base_balances AS (

{% if is_incremental() %}
SELECT
    block_date, address, balance, _inserted_timestamp
FROM
    incremental
{% else %}
SELECT
    block_date, address, balance, _inserted_timestamp
FROM
    fr_balances
{% endif %}),
address_ranges AS (
    SELECT
        address,
        MIN(
            block_date :: DATE
        ) AS min_block_date,
        max_block AS max_block_date
    FROM
        base_balances
        JOIN info
    GROUP BY
        address,
        max_block
),
cte_my_date AS (
    SELECT
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
),
all_dates AS (
    SELECT
        C.block_date,
        A.address
    FROM
        cte_my_date C
        LEFT JOIN address_ranges A
        ON C.block_date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.address IS NOT NULL
),
eth_balances AS (
    SELECT
        address,
        block_date,
        balance,
        _inserted_timestamp,
        TRUE AS daily_activity
    FROM
        base_balances
),
balance_tmp AS (
    SELECT
        d.block_date,
        d.address,
        b.balance,
        b._inserted_timestamp,
        b.daily_activity
    FROM
        all_dates d
        LEFT JOIN eth_balances b
        ON d.block_date = b.block_date
        AND d.address = b.address
),
FINAL AS (
    SELECT
        block_date,
        address,
        LAST_VALUE(
            balance ignore nulls
        ) over(
            PARTITION BY address
            ORDER BY
                block_date ASC rows unbounded preceding
        ) AS balance,
        LAST_VALUE(
            _inserted_timestamp ignore nulls
        ) over(
            PARTITION BY address
            ORDER BY
                block_date ASC rows unbounded preceding
        ) AS _inserted_timestamp,
        CASE
            WHEN daily_activity IS NULL THEN FALSE
            ELSE TRUE
        END AS daily_activity,
        {{ dbt_utils.surrogate_key(
            ['block_date', 'address']
        ) }} AS id
    FROM
        balance_tmp
)
SELECT
    *
FROM
    FINAL
WHERE
    balance <> 0
