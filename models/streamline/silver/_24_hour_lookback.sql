{{ config (
    materialized = "ephemeral"
) }}

SELECT
    MIN(block_number) AS block_number
FROM
    {{ ref("silver__blocks") }}
WHERE
    block_timestamp BETWEEN DATEADD('hour', -25, SYSDATE())
    AND DATEADD('hour', -24, SYSDATE())
