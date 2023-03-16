{{ config(
    materialized = 'incremental',
    cluster_by = ['block_timestamp::date', 'pool_address'],
    unique_key = 'id'
) }}

WITH block_date AS (

    SELECT
        block_timestamp,
        block_number
    FROM
        {{ ref("silver__blocks") }}
),
base_pool_data AS (
    SELECT
        A.*,
        b.block_timestamp,
        segmented_data AS segmented_output
    FROM
        {{ ref('bronze__successful_reads') }} A
        JOIN block_date b
        ON A.block_number = b.block_number
    WHERE
        function_signature IN (
            '0x1ad8b03b',
            '0x1a686502',
            '0x46141319',
            '0x3850c7bd',
            '0xf3058399'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
pool_meta AS (
    SELECT
        token0_address,
        token1_address,
        fee,
        fee_percent,
        tick_spacing,
        pool_address,
        token0_symbol,
        token1_symbol,
        token0_decimals,
        token1_decimals,
        pool_name
    FROM
        {{ ref('silver__univ3_pools') }}
),
protocol_fees_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) :: FLOAT AS token0_protocol_fees,
        PUBLIC.udf_hex_to_int(
            segmented_output [1] :: STRING
        ) :: FLOAT AS token1_protocol_fees
    FROM
        base_pool_data
    WHERE
        function_signature = '0x06fdde03'
),
liquidity_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) :: FLOAT AS liquidity
    FROM
        base_pool_data
    WHERE
        function_signature = '0x1a686502'
),
feeGrowthGlobal1X128_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) :: FLOAT AS feeGrowthGlobal1X128
    FROM
        base_pool_data
    WHERE
        function_signature = '0x46141319'
),
feeGrowthGlobal0X128_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) :: FLOAT AS feeGrowthGlobal0X128
    FROM
        base_pool_data
    WHERE
        function_signature = '0xf3058399'
),
slot0_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) :: FLOAT AS sqrtPriceX96,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_output [1] :: STRING
        ) :: FLOAT AS tick,
        PUBLIC.udf_hex_to_int(
            segmented_output [2] :: STRING
        ) :: FLOAT AS observationIndex,
        PUBLIC.udf_hex_to_int(
            segmented_output [3] :: STRING
        ) :: FLOAT AS observationCardinality,
        PUBLIC.udf_hex_to_int(
            segmented_output [4] :: STRING
        ) :: FLOAT AS observationCardinalityNext,
        PUBLIC.udf_hex_to_int(
            segmented_output [5] :: STRING
        ) :: FLOAT AS feeProtocol,
        PUBLIC.udf_hex_to_int(
            segmented_output [6] :: STRING
        ) :: FLOAT AS unlocked
    FROM
        base_pool_data
    WHERE
        function_signature = '0x3850c7bd'
),
contract_range AS (
    SELECT
        block_number,
        block_timestamp,
        contract_address,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_pool_data
    GROUP BY
        1,
        2,
        3
),
join_meta AS (
    SELECT
        c1.block_number AS block_number,
        c1.contract_address AS pool_address,
        c1.block_timestamp AS block_timestamp,
        COALESCE(
            token0_protocol_fees / pow(
                10,
                token0_decimals
            ),
            0
        ) AS protocol_fees_token0_adjusted,
        COALESCE(
            token1_protocol_fees / pow(
                10,
                token1_decimals
            ),
            0
        ) AS protocol_fees_token1_adjusted,
        liquidity,
        COALESCE(
            liquidity / pow(
                10,
                (
                    token1_decimals + token0_decimals
                ) / 2
            ),
            0
        ) AS virtual_liquidity_adjusted,
        feeGrowthGlobal1X128,
        feeGrowthGlobal0X128,
        sqrtPriceX96,
        tick,
        observationIndex,
        observationCardinality,
        observationCardinalityNext,
        feeProtocol,
        CASE
            WHEN unlocked = 1 THEN TRUE
            ELSE FALSE
        END AS unlocked,
        CONCAT(
            c1.block_number,
            '-',
            c1.contract_address
        ) AS id,
        c1._inserted_timestamp AS _inserted_timestamp,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        ((sqrtpricex96 * sqrtpricex96)) / pow(
            2,
            192
        ) AS human_price,
        SQRT(human_price) AS sqrt_hp,
        div0(
            liquidity,
            sqrt_hp
        ) / pow(
            10,
            token0_decimals
        ) AS virtual_reserves_token0_adjusted,
        (
            liquidity * sqrt_hp
        ) / pow(
            10,
            token1_decimals
        ) AS virtual_reserves_token1_adjusted,
        token0_decimals,
        token1_decimals,
        pool_name
    FROM
        contract_range c1
        LEFT JOIN protocol_fees_base
        ON c1.contract_address = protocol_fees_base.contract_address
        AND c1.block_number = protocol_fees_base.block_number
        LEFT JOIN liquidity_base
        ON c1.contract_address = liquidity_base.contract_address
        AND c1.block_number = liquidity_base.block_number
        LEFT JOIN feeGrowthGlobal1X128_base
        ON c1.contract_address = feeGrowthGlobal1X128_base.contract_address
        AND c1.block_number = feeGrowthGlobal1X128_base.block_number
        LEFT JOIN feeGrowthGlobal0X128_base
        ON c1.contract_address = feeGrowthGlobal0X128_base.contract_address
        AND c1.block_number = feeGrowthGlobal0X128_base.block_number
        LEFT JOIN slot0_base
        ON c1.contract_address = slot0_base.contract_address
        AND c1.block_number = slot0_base.block_number
        LEFT JOIN pool_meta
        ON c1.contract_address = pool_address
),
token_balances AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour,
        address,
        contract_address,
        AVG(
            balance :: FLOAT
        ) AS balance
    FROM
        {{ ref('silver__token_balances') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND address IN (
            SELECT
                DISTINCT pool_address
            FROM
                pool_meta
        )
        AND block_timestamp :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                join_meta
        )
    GROUP BY
        1,
        2,
        3
),
daily_balances AS (
    SELECT
        block_hour :: DATE AS block_date,
        address,
        contract_address,
        AVG(balance) AS daily_balance
    FROM
        token_balances
    GROUP BY
        1,
        2,
        3
),
max_balances AS (
    SELECT
        address,
        contract_address,
        MAX(balance) AS max_balance
    FROM
        token_balances
    GROUP BY
        1,
        2
)
SELECT
    A.*,
    COALESCE(
        b0.balance,
        db0.daily_balance,
        mb0.max_balance,
        0
    ) AS token0_balance,
    COALESCE(
        b1.balance,
        db1.daily_balance,
        mb1.max_balance,
        0
    ) AS token1_balance,
    token0_balance / pow(
        10,
        token0_decimals
    ) AS token0_balance_adjusted,
    token1_balance / pow(
        10,
        token1_decimals
    ) AS token1_balance_adjusted,
    pow(
        1.0001,
        tick
    ) / pow(
        10,
        token1_decimals - token0_decimals
    ) AS price_1_0,
    1 / price_1_0 AS price_0_1
FROM
    join_meta A
    LEFT JOIN token_balances AS b0
    ON A.pool_address = b0.address
    AND A.token0_address = b0.contract_address
    AND DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = b0.block_hour
    LEFT JOIN token_balances AS b1
    ON A.pool_address = b1.address
    AND A.token1_address = b1.contract_address
    AND DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = b1.block_hour
    LEFT JOIN daily_balances db0
    ON A.pool_address = db0.address
    AND A.token0_address = db0.contract_address
    AND A.block_timestamp :: DATE = db0.block_date
    LEFT JOIN daily_balances db1
    ON A.pool_address = db1.address
    AND A.token1_address = db1.contract_address
    AND A.block_timestamp :: DATE = db1.block_date
    LEFT JOIN max_balances mb0
    ON A.pool_address = mb0.address
    AND A.token0_address = mb0.contract_address
    LEFT JOIN max_balances mb1
    ON A.pool_address = mb1.address
    AND A.token1_address = mb1.contract_address qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    A._inserted_timestamp DESC)) = 1
