{{ config(
    materialized = 'incremental',
    unique_key = "id",
    incremental_strategy = 'delete+insert',
    tags = ['non_realtime']
) }}

WITH base AS (

    SELECT
        block_number,
        contract_address,
        function_input AS token_address,
        read_output,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature = '0x35ea6a75'
        AND read_output :: STRING <> '0x'

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
decoded AS (
    SELECT
        *,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS availableLiquidity,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: FLOAT AS totalStableDebt,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: FLOAT AS totalVariableDebt,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: FLOAT AS liquidityRate,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: FLOAT AS variableBorrowRate,
        utils.udf_hex_to_int(
            segmented_data [5] :: STRING
        ) :: FLOAT AS stableBorrowRate,
        utils.udf_hex_to_int(
            segmented_data [6] :: STRING
        ) :: FLOAT AS averageStableBorrowRate,
        utils.udf_hex_to_int(
            segmented_data [7] :: STRING
        ) :: FLOAT AS liquidityIndex,
        utils.udf_hex_to_int(
            segmented_data [8] :: STRING
        ) :: FLOAT AS variableBorrowIndex,
        utils.udf_hex_to_int(
            segmented_data [9] :: STRING
        ) :: FLOAT AS lastUpdateTimestamp
    FROM
        base
)
SELECT
    block_number,
    contract_address,
    token_address,
    _inserted_timestamp,
    availableLiquidity,
    totalStableDebt,
    totalVariableDebt,
    liquidityRate,
    variableBorrowRate,
    stableBorrowRate,
    averageStableBorrowRate,
    liquidityIndex,
    variableBorrowIndex,
    lastUpdateTimestamp,
    concat_ws(
        block_number,
        contract_address,
        token_address
    ) AS id
FROM
    decoded qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
