{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__confirmed_blocks') }}
