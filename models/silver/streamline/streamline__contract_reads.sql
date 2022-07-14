{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_view']
) }}

WITH all_reads AS (

    SELECT
        id :: STRING AS id,
        block_number :: INTEGER AS block_number,
        contract_address :: STRING AS contract_address,
        call_name :: STRING AS call_name,
        function_signature :: STRING AS function_signature,
        function_input :: STRING AS function_input,
        _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
    FROM
        {{ ref('streamline__curve_pool_reads') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__aave_incentives_read') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__aave_prices_oracle_read') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__aave_reserve_data_read') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__compound_comptroller_stats') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__compound_market_stats') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__uni_v3_pool_reads') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__uni_v3_position_reads') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__uni_v3_token_reads') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    id,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input,
    _inserted_timestamp
FROM
    all_reads
