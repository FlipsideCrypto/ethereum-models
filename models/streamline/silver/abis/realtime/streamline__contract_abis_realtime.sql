{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_contract_abis()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
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
FINAL AS (
    SELECT
        created_contract_address AS contract_address,
        block_number
    FROM
        {{ ref("silver__created_contracts") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        contract_address,
        block_number
    FROM
        {{ ref("streamline__complete_contract_abis") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
)
SELECT
    *
FROM
    (
        SELECT
            contract_address,
            block_number
        FROM
            FINAL
        UNION ALL
        SELECT
            contract_address,
            block_number
        FROM
            {{ ref("_retry_abis") }}
        WHERE
            block_number IS NOT NULL
    )
WHERE
    contract_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    block_number DESC)) = 1
