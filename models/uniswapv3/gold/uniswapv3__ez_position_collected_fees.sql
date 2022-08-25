{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
FROM
    {{ ref('silver__uni_v3_position_collected_fees') }}
