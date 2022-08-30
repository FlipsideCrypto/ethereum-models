{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_number, token_address)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ethereum', 'aave', 'aave_liquidity_mining']
) }}

WITH aave_base AS (

    SELECT
        block_number,
        function_signature,
        read_output,
        function_input AS token_address,
        _inserted_timestamp,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS emission_per_second
    FROM
        {{ ref('bronze__aave_liquidity_mining') }}
    WHERE
        read_output :: STRING <> '0x'

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
)
SELECT
    block_number,
    token_address,
    emission_per_second,
    _inserted_timestamp
FROM
    aave_base qualify(ROW_NUMBER() over(PARTITION BY block_number, token_address
ORDER BY
    _inserted_timestamp DESC)) = 1
