{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_reads(object_construct('node_name','quicknode'))",
        target = "{{this.schema}}.{{this.identifier}}"
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
)
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
