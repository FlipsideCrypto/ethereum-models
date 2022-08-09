{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_date'],
    tags = ['balances'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH

{% if is_incremental() %}
latest_balance_reads AS (

    SELECT
        block_date,
        address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_balances') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) :: DATE - 2
            FROM
                {{ this }}
        )
),
latest_records AS (
    SELECT
        block_date,
        user_address AS address,
        contract_address,
        balance,
        _inserted_timestamp
    FROM
        {{ this }}
    WHERE
        block_date = (
            SELECT
                DATEADD('day', -0, MAX(block_date))
            FROM
                {{ this }})
        ),
        incremental AS (
            SELECT
                block_date,
                address,
                contract_address,
                balance,
                _inserted_timestamp
            FROM
                (
                    SELECT
                        block_date,
                        address,
                        contract_address,
                        balance,
                        _inserted_timestamp,
                        1 AS RANK
                    FROM
                        latest_balance_reads
                    UNION
                    SELECT
                        block_date,
                        address,
                        contract_address,
                        balance,
                        _inserted_timestamp,
                        2 AS RANK
                    FROM
                        latest_records
                ) qualify(ROW_NUMBER() over(PARTITION BY address, contract_address, block_date
            ORDER BY
                RANK ASC)) = 1
        ),
    {% endif %}

    base_balances AS (

{% if is_incremental() %}
SELECT
    block_date, address, contract_address, balance, _inserted_timestamp
FROM
    incremental
{% else %}
SELECT
    block_date, address, contract_address, balance, _inserted_timestamp
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
        balance,
        _inserted_timestamp
    FROM
        base_balances
),
balance_tmp AS (
    SELECT
        d.block_date,
        d.address,
        d.contract_address,
        b.balance,
        d.blockchain,
        b._inserted_timestamp
    FROM
        all_dates d
        LEFT JOIN eth_balances b
        ON d.block_date = b.block_date
        AND d.address = b.address
        AND d.contract_address = b.contract_address
        AND d.blockchain = b.blockchain
),
balances_final AS (
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
        ) AS balance,
        LAST_VALUE(
            _inserted_timestamp ignore nulls
        ) over(
            PARTITION BY address,
            contract_address,
            blockchain
            ORDER BY
                block_date ASC rows unbounded preceding
        ) AS _inserted_timestamp
    FROM
        balance_tmp
),
token_metadata AS (
    SELECT
        LOWER(address) AS token_address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
),
token_prices AS (
    SELECT
        HOUR :: DATE AS daily_price,
        LOWER(token_address) AS token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IS NOT NULL
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_date :: DATE
            FROM
                balances_final
        )
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        block_date,
        address,
        contract_address,
        symbol,
        decimals,
        NAME,
        blockchain,
        balance AS balance_unadj,
        CASE
            WHEN decimals IS NOT NULL THEN balance / pow(
                10,
                decimals
            )
            ELSE NULL
        END AS balance_adj,
        CASE
            WHEN decimals IS NOT NULL THEN balance_adj * price
        END AS balance_usd,
        price,
        _inserted_timestamp
    FROM
        balances_final A
        LEFT JOIN token_metadata b
        ON A.contract_address = b.token_address
        LEFT JOIN token_prices C
        ON A.contract_address = C.token_address
        AND A.block_date :: DATE = C.daily_price :: DATE
    WHERE
        balance <> 0
)
SELECT
    block_date :: DATE AS block_date,
    address AS user_address,
    contract_address,
    symbol,
    decimals,
    NAME,
    blockchain,
    balance_unadj AS non_adjusted_balance,
    balance_adj :: FLOAT AS balance,
    ROUND(
        balance_usd,
        2
    ) AS balance_usd,
    _inserted_timestamp,
    id,
    CASE
        WHEN decimals IS NULL THEN FALSE
        ELSE TRUE
    END AS has_decimal,
    CASE
        WHEN price IS NULL THEN FALSE
        ELSE TRUE
    END AS has_price,
    {{ dbt_utils.surrogate_key(
        ['block_date', 'contract_address', 'address']
    ) }} AS id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY address, contract_address, block_date
ORDER BY
    _inserted_timestamp DESC)) = 1
