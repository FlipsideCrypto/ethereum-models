{{ config (
<<<<<<<< HEAD:models/streamline/silver/abis/realtime/streamline__contract_abis_realtime.sql
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_contract_abis()",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_abis_realtime']
========
    materialized = "view"
>>>>>>>> main:models/silver/streamline/abis/streamline__contract_abis_history.sql
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
    {{ dbt_utils.generate_surrogate_key(
        ['created_contract_address', 'block_number']
    ) }} AS id,
    created_contract_address AS contract_address,
    block_number
FROM
    {{ ref("silver__created_contracts") }}
WHERE
    block_number < (
        SELECT
            block_number
        FROM
            last_3_days
    )
EXCEPT
SELECT
    id,
    contract_address,
    block_number
FROM
    {{ ref("streamline__complete_contract_abis") }}
WHERE
    block_number < (
        SELECT
            block_number
        FROM
            last_3_days
    )
ORDER BY
    block_number
