{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    label
FROM
    {{ source(
        'flipside_gold',
        'labels'
    ) }}
WHERE
    blockchain = 'ethereum'
