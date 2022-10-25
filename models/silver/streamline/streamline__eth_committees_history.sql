{{ config(
    materialized = 'view'
) }}

SELECT
    func_type,
    block_number AS slot_number
FROM
    {{ source(
        'bronze_streamline',
        'committees'
    ) }}
