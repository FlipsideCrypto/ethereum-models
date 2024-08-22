{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"reads_v2",
        "sql_limit" :"15000000",
        "producer_batch_size" :"15000000",
        "worker_batch_size" :"2500000",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_reads_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
to_do AS (
    SELECT
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
    FROM
        {{ ref("streamline__contract_reads") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
    FROM
        {{ ref("streamline__complete_reads") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    contract_address,
    function_signature,
    call_name,
    function_input,
    block_number,
    CASE
        WHEN function_input IS NULL THEN function_signature
        WHEN function_input ILIKE '0x%' THEN CONCAT(
            function_signature,
            LPAD(SUBSTR(function_input, 3), 64, 0)
        )
        ELSE CONCAT(
            function_signature,
            LPAD(
                function_input,
                64,
                0
            )
        )
    END AS DATA,
    CONCAT((SYSDATE() :: DATE) :: STRING, '_', function_signature) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'eth_call',
            'params',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'to',
                    contract_address,
                    'data',
                    DATA
                ),
                utils.udf_int_to_hex(block_number)
            )
        ),
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS request
FROM
    to_do
ORDER BY
    block_number DESC
