{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_eth_balances()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}


SELECT 
    block_number
FROM 
    generate_series(0, 15491074) block_number
EXCEPT
SELECT
    block_number
FROM
    {{ ref("streamline__complete_eth_v1_blocks") }}
