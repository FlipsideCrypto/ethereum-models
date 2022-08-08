{{ config(
    materialized = 'view'
) }}

WITH all_reads AS (

    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__curve_pool_reads') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__aave_incentives_read') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__aave_prices_oracle_read') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__aave_reserve_data_read') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__compound_comptroller_stats') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__compound_market_stats') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__uni_v3_pool_reads') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__uni_v3_position_reads') }}
    UNION
    SELECT
        id,
        block_number,
        contract_address,
        call_name,
        function_signature,
        function_input,
        _inserted_timestamp
    FROM
        {{ ref('streamline__uni_v3_token_reads') }}
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
