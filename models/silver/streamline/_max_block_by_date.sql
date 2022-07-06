{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}

SELECT
    block_timestamp :: DATE AS block_date,
    MAX(block_number) block_number
FROM
    {{ ref("core__fact_blocks") }}
GROUP BY
    block_timestamp :: DATE
