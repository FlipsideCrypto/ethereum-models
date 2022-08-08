{{ config(
    materialized = 'incremental',
    unique_key = 'address || block_date || contract_address',
    cluster_by = ['block_date'],
    tags = ['balances']
) }}

WITH

{% if is_incremental() %}
latest AS (

    SELECT
        block_date,
        address,
        contract_address,
        balance
    FROM
        {{ this }}
    WHERE
        block_date = (
            SELECT
                DATEADD('day', -0, MAX(block_date))
            FROM
                {{ this }})
        ),
        NEW AS (
            SELECT
                block_date,
                address,
                contract_address,
                balance,
                1 AS RANK
            FROM
                {{ ref('silver__token_balances') }}
            WHERE
                block_date >= (
                    SELECT
                        DATEADD('day', -0, MAX(block_date))
                    FROM
                        {{ this }}) qualify(ROW_NUMBER() over(PARTITION BY address, contract_address, block_date
                    ORDER BY
                        block_date DESC)) = 1
                ),
                incremental AS (
                    SELECT
                        block_date,
                        address,
                        contract_address,
                        balance
                    FROM
                        (
                            SELECT
                                block_date,
                                address,
                                contract_address,
                                balance,
                                2 AS RANK
                            FROM
                                latest
                            UNION
                            SELECT
                                block_date,
                                address,
                                contract_address,
                                balance,
                                1 AS RANK
                            FROM
                                NEW
                        )
                ),
            {% endif %}

            base_balances AS (

{% if is_incremental() %}
SELECT
    block_date, address, contract_address, balance
FROM
    incremental
{% else %}
SELECT
    block_date, address, contract_address, balance
FROM
    {{ ref('silver__token_balances') }}
{% endif %}),
address_ranges AS (
    SELECT
        address,
        contract_address,
        'ethereum' AS blockchain,
        MIN(
            block_date :: DATE
        ) AS min_block_date,
        MAX(
            CURRENT_TIMESTAMP :: DATE
        ) AS max_block_date
    FROM
        base_balances
    GROUP BY
        1,
        2,
        3
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
        A.address,
        A.contract_address,
        A.blockchain
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
        contract_address,
        block_date,
        'ethereum' AS blockchain,
        balance
    FROM
        base_balances qualify(ROW_NUMBER() over(PARTITION BY address, contract_address, block_date
    ORDER BY
        block_date DESC)) = 1
),
balance_tmp AS (
    SELECT
        d.block_date,
        d.address,
        d.contract_address,
        b.balance,
        d.blockchain
    FROM
        all_dates d
        LEFT JOIN eth_balances b
        ON d.block_date = b.block_date
        AND d.address = b.address
        AND d.contract_address = b.contract_address
        AND d.blockchain = b.blockchain
)
SELECT
    block_date,
    address,
    contract_address,
    blockchain,
    LAST_VALUE(
        balance ignore nulls
    ) over(
        PARTITION BY address,
        contract_address,
        blockchain
        ORDER BY
            block_date ASC rows unbounded preceding
    ) AS balance
FROM
    balance_tmp b
