{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view'],
    enabled = false
) }}

WITH block_number AS (

    SELECT
        block_timestamp :: DATE AS DAY,
        MIN(block_number) AS block_number
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        DAY >= '2020-02-01'

{% if is_incremental() %}
AND DAY >= (
    SELECT
        MAX(
            DAY
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
),
daily_price AS (
    SELECT
        recorded_hour :: DATE AS DAY,
        token_address,
        symbol,
        AVG(OPEN) daily_price
    FROM
        (
            SELECT
                A.recorded_hour,
                A.open,
                b.token_address,
                b.symbol
            FROM
                {{ source(
                    'crosschain_silver',
                    'hourly_prices_coin_gecko'
                ) }} A
                JOIN {{ source(
                    'crosschain_silver',
                    'asset_metadata_coin_gecko'
                ) }}
                b
                ON A.id = b.id
            WHERE
                recorded_hour :: DATE = CURRENT_DATE -1
                AND b.token_address IS NOT NULL
                AND b.platform = 'ethereum'
        )
    GROUP BY
        1,
        2,
        3
),
pools AS (
    SELECT
        DISTINCT pool_address
    FROM
        {{ ref("core__dim_dex_liquidity_pools") }}
    WHERE
        platform IN(
            'sushiswap',
            'uniswap-v2'
        )
),
balance AS (
    SELECT
        block_timestamp :: DATE AS DATE,
        address,
        contract_address,
        MIN(balance) AS balance
    FROM
        {{ ref("silver__token_balances") }}
    WHERE
        address IN (
            SELECT
                *
            FROM
                pools
        )
        AND block_timestamp :: DATE = CURRENT_DATE -1
    GROUP BY
        1,
        2,
        3
),
decimals AS (
    SELECT
        address,
        decimals
    FROM
        {{ ref("core__dim_contracts") }}
),
pools_value AS (
    SELECT
        C.date,
        C.address,
        C.contract_address,
        d.day,
        d.symbol,
        C.balance * d.daily_price / power(
            10,
            e.decimals
        ) AS value_USD
    FROM
        balance C
        LEFT JOIN daily_price d
        ON C.date = d.day
        AND C.contract_address = d.token_address
        LEFT JOIN decimals e
        ON C.contract_address = e.address
),
top_pools AS (
    SELECT
        DATE,
        address,
        SUM(value_usd) AS pool_value
    FROM
        pools_value
    GROUP BY
        1,
        2 qualify ROW_NUMBER() over(
            ORDER BY
                ZEROIFNULL(pool_value) DESC
        ) <= 200
),
balance_of_slp_reads AS (
    SELECT
        DAY,
        block_number,
        address AS contract_address,
        '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' AS function_input,
        '0x70a08231' AS function_signature
    FROM
        block_number
        JOIN top_pools
),
total_slp_supply_reads AS (
    SELECT
        DAY,
        block_number,
        address AS contract_address,
        '' AS function_input,
        '0x18160ddd' AS function_signature
    FROM
        block_number
        JOIN top_pools
),
FINAL AS (
    SELECT
        DAY,
        block_number,
        contract_address,
        'Balance_of_SLP_staked' call_name,
        function_input,
        function_signature
    FROM
        balance_of_slp_reads
    UNION ALL
    SELECT
        DAY,
        block_number,
        contract_address,
        'Total_SLP_issued' call_name,
        function_input,
        function_signature
    FROM
        total_slp_supply_reads
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    DAY,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    FINAL
