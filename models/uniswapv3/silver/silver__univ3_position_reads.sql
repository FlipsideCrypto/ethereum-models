{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_number']
) }}

WITH position_reads_base AS (

    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(function_input) AS nf_token_id,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS nonce,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) AS OPERATOR,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 27, 40)) AS token1,
        PUBLIC.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) AS fee,
        PUBLIC.udf_hex_to_int(
            segmented_data [5] :: STRING
        ) AS tickLower,
        PUBLIC.udf_hex_to_int(
            segmented_data [6] :: STRING
        ) AS tickUpper,
        PUBLIC.udf_hex_to_int(
            segmented_data [7] :: STRING
        ) AS liquidity,
        PUBLIC.udf_hex_to_int(
            segmented_data [8] :: STRING
        ) AS feeGrowthInside0LastX128,
        PUBLIC.udf_hex_to_int(
            segmented_data [9] :: STRING
        ) AS feeGrowthInside1LastX128,
        PUBLIC.udf_hex_to_int(
            segmented_data [10] :: STRING
        ) AS tokensOwed0,
        PUBLIC.udf_hex_to_int(
            segmented_data [11] :: STRING
        ) AS tokensOwed1,
        CONCAT(
            contract_address,
            '-',
            block_number,
            '-',
            function_input
        ) AS id,
        _inserted_timestamp
    FROM
        {{ ref('bronze__univ3_position_reads') }}
)
SELECT
    *
FROM
    position_reads_base qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
