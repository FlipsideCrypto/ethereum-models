{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::date']
) }}

WITH lp_events AS (

    SELECT
        blockchain,
        block_number,
        block_timestamp,
        tx_hash,
        liquidity_provider,
        nf_position_manager_address,
        nf_token_id,
        pool_address,
        tick_upper,
        tick_lower,
        token0_address,
        token1_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__univ3_lp_actions') }}
    WHERE
        nf_token_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
position_reads_base AS (
    SELECT
        contract_address,
        block_number,
        PUBLIC.udf_hex_to_int(CONCAT('0x', function_input :: STRING)) :: STRING AS nf_token_id,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS nonce,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) AS OPERATOR,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 27, 40)) AS token1,
        PUBLIC.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: FLOAT AS fee,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) :: FLOAT AS tickLower,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: FLOAT AS tickUpper,
        PUBLIC.udf_hex_to_int(
            segmented_data [7] :: STRING
        ) :: FLOAT AS liquidity,
        PUBLIC.udf_hex_to_int(
            segmented_data [8] :: STRING
        ) :: FLOAT AS feeGrowthInside0LastX128,
        PUBLIC.udf_hex_to_int(
            segmented_data [9] :: STRING
        ) :: FLOAT AS feeGrowthInside1LastX128,
        PUBLIC.udf_hex_to_int(
            segmented_data [10] :: STRING
        ) :: FLOAT AS tokensOwed0,
        PUBLIC.udf_hex_to_int(
            segmented_data [11] :: STRING
        ) :: FLOAT AS tokensOwed1
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature = '0x99fbab88'

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
pool_data AS (
    SELECT
        token0_address,
        token1_address,
        tick_spacing,
        pool_address
    FROM
        {{ ref('silver__univ3_pools') }}
),
FINAL AS (
    SELECT
        blockchain,
        A.block_number,
        block_timestamp,
        tx_hash,
        COALESCE(
            fee,
            0
        ) AS fee,
        COALESCE(
            fee,
            0
        ) / 10000 AS fee_percent,
        COALESCE(
            feeGrowthInside0LastX128,
            0
        ) AS fee_growth_inside0_last_x128,
        COALESCE(
            feeGrowthInside1LastX128,
            0
        ) AS fee_growth_inside1_last_x128,
        CASE
            WHEN fee_percent <> 0 THEN TRUE
            ELSE FALSE
        END AS is_active,
        liquidity,
        liquidity_provider,
        nf_position_manager_address,
        A.nf_token_id,
        A.pool_address,
        COALESCE(
            tick_upper,
            0
        ) AS tick_upper,
        COALESCE(
            tick_lower,
            0
        ) AS tick_lower,
        tokensOwed0,
        tokensOwed1,
        A.token0_address,
        A.token1_address,
        _log_id,
        _inserted_timestamp
    FROM
        lp_events A
        LEFT JOIN position_reads_base b
        ON A.block_number = b.block_number
        AND A.nf_token_id :: STRING = b.nf_token_id :: STRING
        LEFT JOIN pool_data C
        ON A.pool_address = C.pool_address
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
