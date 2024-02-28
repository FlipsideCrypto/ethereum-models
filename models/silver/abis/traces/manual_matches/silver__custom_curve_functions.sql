{{ config (
    materialized = 'table',
    tags = ['static']
) }}
-- custom logic to add several functions to the complete_function_abis table for curve pools
WITH pools_base AS (

    SELECT
        DISTINCT pool_address
    FROM
        {{ ref('silver_dex__curve_pools') }}
    UNION
    SELECT
        '0xf5f5b97624542d72a9e06f04804bf81baa15e2b4' AS pool_address -- this pool is not in the curve_pools table
),
manual_functions AS (
    SELECT
        DISTINCT function_name,
        simple_function_name,
        function_signature,
        inputs,
        inputs_type,
        outputs,
        outputs_type,
        abi
    FROM
        {{ ref('silver__flat_function_abis') }}
    WHERE
        contract_address IN (
            SELECT
                DISTINCT contract_address
            FROM
                {{ ref('silver__flat_function_abis') }}
            WHERE
                function_name = 'reduction_coefficient'
        ) qualify ROW_NUMBER() over (
            PARTITION BY function_signature
            ORDER BY
                inputs DESC
        ) = 1
)
SELECT
    pool_address AS contract_address,
    function_name,
    simple_function_name,
    function_signature,
    inputs,
    inputs_type,
    outputs,
    outputs_type,
    abi
FROM
    pools_base
    JOIN manual_functions
    ON 1 = 1
