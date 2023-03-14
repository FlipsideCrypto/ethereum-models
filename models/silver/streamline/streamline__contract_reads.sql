{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

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
UNION ALL
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
UNION ALL
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
UNION ALL
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
UNION ALL
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
UNION ALL
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
UNION ALL
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
UNION ALL
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
UNION ALL
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
<<<<<<< HEAD
    {{ ref('streamline__contract_meta_read') }}
=======
    {{ ref('streamline__chainlink_prices') }}
>>>>>>> 6f21f2454d2f9dab42e2addc94be0710ac35570d
UNION ALL
SELECT
    id :: STRING AS id,
    block_number :: INTEGER AS block_number,
    contract_address :: STRING AS contract_address,
    call_name :: STRING AS call_name,
    function_signature :: STRING AS function_signature,
    function_input :: STRING AS function_input,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
    {{ ref('streamline__aave_reserve_config_reads') }}
