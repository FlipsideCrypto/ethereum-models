{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
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
        'flipside_gold_ethereum',
        'labels'
    ) }}
WHERE
    blockchain = 'ethereum'
    AND address LIKE '0x%'
