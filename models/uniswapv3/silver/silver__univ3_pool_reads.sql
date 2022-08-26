{{ config(
    materialized = 'incremental',
    cluster_by = ['contract_address'],
    unique_key = 'id'
) }}

WITH

{% if is_incremental() %}
heal_table AS (

    SELECT
        contract_address
    FROM
        {{ this }}
    WHERE
        token0_protocol_fees IS NULL
        OR token1_protocol_fees IS NULL
        OR liquidity IS NULL
        OR feeGrowthGlobal1X128 IS NULL
        OR feeGrowthGlobal0X128 IS NULL
        OR sqrtPriceX96 IS NULL
        OR tick IS NULL
        OR observationIndex IS NULL
        OR observationCardinality IS NULL
        OR observationCardinalityNext IS NULL
        OR feeProtocol IS NULL
        OR unlocked IS NULL
),
{% endif %}

base_pool_data AS (
    SELECT
        *
    FROM
        {{ ref('bronze__univ3_pool_reads') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE - 2
        FROM
            {{ this }}
    )
    OR contract_address IN (
        SELECT
            DISTINCT contract_address
        FROM
            heal_table
    )
{% endif %}
),
protocol_fees_base AS (
    SELECT
        contract_address,
        block_number,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) AS token0_protocol_fees,
        PUBLIC.udf_hex_to_int(
            segmented_output [1] :: STRING
        ) AS token1_protocol_fees
    FROM
        base_pool_data
    WHERE
        function_signature = '0x06fdde03'
),
liquidity_base AS (
    SELECT
        contract_address,
        block_number,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) AS liquidity
    FROM
        base_pool_data
    WHERE
        function_signature = '0x1a686502'
),
feeGrowthGlobal1X128_base AS (
    SELECT
        contract_address,
        block_number,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) AS feeGrowthGlobal1X128
    FROM
        base_pool_data
    WHERE
        function_signature = '0x46141319'
),
feeGrowthGlobal0X128_base AS (
    SELECT
        contract_address,
        block_number,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) AS feeGrowthGlobal0X128
    FROM
        base_pool_data
    WHERE
        function_signature = '0xf3058399'
),
slot0_base AS (
    SELECT
        contract_address,
        block_number,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        PUBLIC.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) AS sqrtPriceX96,
        PUBLIC.udf_hex_to_int(
            segmented_output [1] :: STRING
        ) AS tick,
        PUBLIC.udf_hex_to_int(
            segmented_output [2] :: STRING
        ) AS observationIndex,
        PUBLIC.udf_hex_to_int(
            segmented_output [3] :: STRING
        ) AS observationCardinality,
        PUBLIC.udf_hex_to_int(
            segmented_output [4] :: STRING
        ) AS observationCardinalityNext,
        PUBLIC.udf_hex_to_int(
            segmented_output [5] :: STRING
        ) AS feeProtocol,
        PUBLIC.udf_hex_to_int(
            segmented_output [6] :: STRING
        ) AS unlocked
    FROM
        base_pool_data
    WHERE
        function_signature = '0x3850c7bd'
),
contract_range AS (
    SELECT
        block_number,
        contract_address,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_pool_data
    GROUP BY
        1,
        2
)
SELECT
    c1.block_number AS block_number,
    c1.contract_address AS contract_address,
    token0_protocol_fees,
    token1_protocol_fees,
    liquidity,
    feeGrowthGlobal1X128,
    feeGrowthGlobal0X128,
    sqrtPriceX96,
    tick,
    observationIndex,
    observationCardinality,
    observationCardinalityNext,
    feeProtocol,
    unlocked,
    CONCAT(
        c1.block_number,
        '-',
        c1.contract_address
    ) AS id,
    c1._inserted_timestamp AS _inserted_timestamp
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
    AND c1.block_number = slot0_base.block_number qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
